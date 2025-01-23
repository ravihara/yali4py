import logging
from abc import ABC
from enum import Enum
from typing import Annotated, Any, ClassVar, Dict, FrozenSet, List, Tuple, Type

import msgspec
import pymongo
from bson import ObjectId
from cachetools import func as cache_func
from pymongo import MongoClient
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor
from pymongo.operations import InsertOne, UpdateMany, UpdateOne
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

from yali.core.codecs import JSONNode
from yali.core.models import BaseModel, field_specs
from yali.core.settings import EnvConfig, env_config
from yali.core.timings import Chrono
from yali.core.typebase import ConstrNode, MongoUrl, PositiveInt, SecretStr

DocId = str | ObjectId
DocOp = InsertOne | UpdateOne | UpdateMany
UniOrMultiDoc = Dict | List[Dict]
InsertResult = InsertManyResult | InsertOneResult
UpsertResult = InsertResult | UpdateResult
IndexHint = List[Tuple[str, int]]

## Field Update Operators
OP_CURRENT_DATE = "$currentDate"
OP_INCREMENT = "$inc"
OP_MIN = "$min"
OP_MAX = "$max"
OP_MULTIPLY = "$mul"
OP_RENAME = "$rename"
OP_SET = "$set"
OP_SET_ON_INSERT = "$setOnInsert"
OP_UNSET = "$unset"

## Array Update Operators
OP_ADD_TO_SET = "$addToSet"
OP_POP = "$pop"
OP_PULL = "$pull"
OP_PULL_ALL = "$pullAll"
OP_PUSH = "$push"

## Bitwise Update Operators
OP_BIT = "$bit"
OP_AND = "$and"
OP_OR = "$or"
OP_XOR = "$xor"
OP_NOT = "$not"

DocUpdateOps: FrozenSet[str] = frozenset(
    [
        OP_CURRENT_DATE,
        OP_INCREMENT,
        OP_MIN,
        OP_MAX,
        OP_MULTIPLY,
        OP_RENAME,
        OP_SET,
        OP_SET_ON_INSERT,
        OP_UNSET,
        OP_ADD_TO_SET,
        OP_POP,
        OP_PULL,
        OP_PULL_ALL,
        OP_PUSH,
        OP_BIT,
        OP_AND,
        OP_OR,
        OP_XOR,
        OP_NOT,
    ]
)


def doc_record_enc_hook(obj: Any) -> Any:
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, SecretStr):
        return obj.get_secret_value()
    elif isinstance(obj, complex):
        return (obj.real, obj.imag)
    elif isinstance(obj, BaseException):
        return str(obj.args[0]) if obj.args else obj.__str__()

    raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


def doc_record_dec_hook(type: Type, obj: Any) -> Any:
    if type is ObjectId:
        return ObjectId(obj)
    elif type is SecretStr:
        return SecretStr(obj)
    elif type is complex:
        return complex(obj[0], obj[1])
    elif issubclass(type, BaseException):
        return type(obj)

    raise NotImplementedError(f"Objects of type {type} are not supported")


class DocIndexConfig(BaseModel):
    name: str | None = None
    unique: bool = False
    sparse: bool = False
    background: bool = False
    expireAfterSeconds: (
        Annotated[int, ConstrNode.constr_num(ge=0, le=2147483647)] | None
    ) = field_specs(default=None)


class TextIndexConfig(DocIndexConfig):
    weights: Dict[str, int] | None = None
    default_language: str = "english"
    language_override: str = "language"
    text_version: int | None = None


class Geo2DIndexConfig(DocIndexConfig):
    bits: Annotated[int, ConstrNode.constr_num(ge=1, le=32)] = field_specs(default=26)
    min: Annotated[float, ConstrNode.constr_num(ge=-180.0, le=180.0)] = field_specs(
        default=-180.0
    )
    max: Annotated[float, ConstrNode.constr_num(ge=-180.0, le=180.0)] = field_specs(
        default=180.0
    )


class IndexQualifier(Enum):
    Ascending = pymongo.ASCENDING
    Descending = pymongo.DESCENDING
    Hashed = pymongo.HASHED
    Text = pymongo.TEXT
    Geo2D = pymongo.GEO2D
    GeoSphere = pymongo.GEOSPHERE


class IndexField(BaseModel):
    name: str
    qualifier: IndexQualifier = IndexQualifier.Ascending


IndexConfig = DocIndexConfig | TextIndexConfig | Geo2DIndexConfig


class DocDbSettings(BaseModel):
    _env_config: ClassVar[EnvConfig] = env_config()

    db_url: MongoUrl | None = _env_config("DB_URL", default=None, cast=MongoUrl)
    db_host: str | None = _env_config("DB_HOST", default=None)
    db_port: PositiveInt | None = _env_config("DB_PORT", default=None, cast=PositiveInt)
    db_username: str = _env_config("DB_USER")
    db_password: SecretStr = _env_config("DB_PASSWD", cast=SecretStr)
    db_pool_size: PositiveInt = _env_config(
        "DB_POOL_SIZE", default=100, cast=PositiveInt
    )
    db_cache_size: PositiveInt = _env_config(
        "DB_CACHE_SIZE", default=128, cast=PositiveInt
    )
    db_cache_ttl: PositiveInt = _env_config(
        "DB_CACHE_TTL", default=600, cast=PositiveInt
    )
    db_max_bulk_ops: PositiveInt = _env_config(
        "DB_MAX_BULK_OPS", default=100, cast=PositiveInt
    )
    docdb_url: str = ""

    def __post_init__(self):
        if not self.db_url and not (self.db_host and self.db_port):
            raise ValueError("Either DB_URL or DB_HOST and DB_PORT must be specified")

        if self.db_url:
            self.docdb_url = str(self.db_url)
        else:
            assert self.db_host and self.db_port
            self.docdb_url = f"mongodb://{self.db_host}:{self.db_port}"


class DocIndex(BaseModel):
    fields: Annotated[
        list[IndexField], ConstrNode.constr_seq(min_length=1, max_length=32)
    ]
    config: IndexConfig = DocIndexConfig


class DocRecord(BaseModel):
    id: DocId | None = field_specs(default=None, name="_id")
    created_at: Chrono.mod.datetime = field_specs(
        default_factory=Chrono.get_current_utc_time
    )
    updated_at: Chrono.mod.datetime = field_specs(
        default_factory=Chrono.get_current_utc_time
    )
    created_by: str | None = None
    updated_by: str | None = None

    rec_encoder: ClassVar[msgspec.json.Encoder] = msgspec.json.Encoder(
        enc_hook=doc_record_enc_hook
    )
    rec_decoder: ClassVar[msgspec.json.Decoder] = msgspec.json.Decoder(
        type=Type["DocRecord"], dec_hook=doc_record_dec_hook
    )


_docdb_settings = DocDbSettings()


class DocRepo(ABC):
    __db_client: MongoClient | None = None

    logger = logging.getLogger("yali.dbstore.docdb")

    @classmethod
    def _init_client(cls):
        if cls.__db_client:
            return cls.__db_client

        cls.__db_client = MongoClient(
            _docdb_settings.docdb_url,
            username=_docdb_settings.db_username,
            password=_docdb_settings.db_password.get_secret_value(),
            authSource="admin",
            maxPoolSize=_docdb_settings.db_pool_size,
        )

        return cls.__db_client

    @staticmethod
    def sync_created_audit(*, doc: Dict, actor: str):
        if "created_by" not in doc:
            doc["created_by"] = actor

        if "created_at" not in doc:
            doc["created_at"] = Chrono.get_current_utc_time()

    @staticmethod
    def sync_updated_audit(*, doc: Dict, actor: str):
        if "updated_by" not in doc:
            doc["updated_by"] = actor

        if "updated_at" not in doc:
            doc["updated_at"] = Chrono.get_current_utc_time()

    @staticmethod
    def is_cursor(entry) -> bool:
        return isinstance(entry, Cursor) or isinstance(entry, CommandCursor)

    @staticmethod
    def is_ops_result(entry) -> bool:
        return (
            isinstance(entry, InsertResult)
            or isinstance(entry, UpdateResult)
            or isinstance(entry, DeleteResult)
            or isinstance(entry, BulkWriteResult)
        )

    def __init__(self, *, db_name: str, coll_name: str, record_cls: Type[DocRecord]):
        self._db_name = db_name
        self._coll_name = coll_name
        self._record_cls = record_cls
        self._ops: List[DocOp] = []

        if not self.__class__.__db_client:
            self.__class__._init_client()

    @property
    def settings(self):
        return _docdb_settings

    @property
    def client(self):
        assert self.__class__.__db_client is not None
        return self.__class__.__db_client

    @property
    def database(self):
        return self.client[self._db_name]

    @property
    def collection(self):
        return self.client[self._db_name][self._coll_name]

    def sync_indexes(self, indexes: List[DocIndex], *, strict: bool = False):
        record_keys = self._record_cls.__annotations__.keys()

        for index in indexes:
            index_entries = []
            is_valid = True

            for item in index.fields:
                if strict and (item.name not in record_keys):
                    self.logger.error(
                        f"Field {item.name} is not defined in the record class. Skipping {JSONNode.dump_str(index)} from indexing."
                    )

                    is_valid = False
                    break

                index_entries.append((item.name, item.qualifier.value))

            if not is_valid or not index_entries:
                index_entries.clear()
                continue

            try:
                self.collection.create_index(
                    index_entries, **JSONNode.load_data(data=index.config)
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to create index {JSONNode.dump_str(index)}. Error: {str(e)}"
                )

    def create(
        self,
        *,
        data: UniOrMultiDoc,
        actor: str = "yali",
    ) -> InsertResult:
        if isinstance(data, list):
            for doc in data:
                self.sync_created_audit(doc=doc, actor=actor)
                self.sync_updated_audit(doc=doc, actor=actor)

            return self.collection.insert_many(documents=data)

        self.sync_created_audit(doc=data, actor=actor)
        self.sync_updated_audit(doc=data, actor=actor)

        return self.collection.insert_one(document=data)

    def update(
        self,
        *,
        filters: Dict,
        data: Dict,
        update_one: bool = False,
        upsert: bool = False,
        actor: str = "yali",
    ) -> UpdateResult:
        op_data: Dict | None = None
        creation_audit = {}
        updation_audit = {}

        for op in DocUpdateOps:
            if op in data:
                op_data = data
                break

        if not op_data:
            self.sync_created_audit(doc=creation_audit, actor=actor)
            op_data = {OP_SET: data, OP_SET_ON_INSERT: creation_audit}

        if OP_SET in op_data:
            self.sync_updated_audit(doc=op_data[OP_SET], actor=actor)
        else:
            self.sync_updated_audit(doc=updation_audit, actor=actor)
            op_data.update({OP_SET: updation_audit})

        if update_one:
            return self.collection.update_one(
                filter=filters, update=op_data, upsert=upsert
            )

        return self.collection.update_many(
            filter=filters, update=op_data, upsert=upsert
        )

    def save(
        self,
        *,
        data: UniOrMultiDoc,
        filters: Dict | None = None,
        upsert: bool = False,
        actor: str = "yali",
    ) -> UpsertResult:
        if not filters:
            return self.create(data=data, actor=actor)

        docs_count = self.collection.count_documents(
            filter=filters, limit=2, maxTimeMS=5000
        )
        update_one = docs_count == 1

        if isinstance(data, list):
            return self.update(
                filters=filters,
                data=data[0],
                update_one=update_one,
                upsert=upsert,
                actor=actor,
            )

        return self.update(
            filters=filters,
            data=data,
            update_one=update_one,
            upsert=upsert,
            actor=actor,
        )

    def delete(self, *, filters: Dict, delete_one: bool = False):
        if delete_one:
            return self.collection.delete_one(filter=filters)

        return self.collection.delete_many(filter=filters)

    def read(
        self,
        *,
        filters: Dict = {},
        projection: Dict | None = None,
        pipeline: List[Dict] | None = None,
        find_one: bool = False,
        index_hint: IndexHint | None = None,
        allow_disk_use: bool = False,
    ):
        aggr_kwargs = {}

        if index_hint:
            aggr_kwargs.update({"hint": index_hint})

        if allow_disk_use:
            aggr_kwargs.update({"allowDiskUse": True})

        read_result = None
        read_cursor: Cursor | CommandCursor | None = None

        if pipeline:
            read_cursor = self.collection.aggregate(pipeline, **aggr_kwargs)
        elif projection:
            if find_one:
                read_result = self.collection.find_one(
                    filter=filters, projection=projection
                )
            else:
                read_cursor = self.collection.find(
                    filter=filters, projection=projection
                )
        else:
            if find_one:
                read_result = self.collection.find_one(filter=filters)
            else:
                read_cursor = self.collection.find(filter=filters)

        if not pipeline and index_hint:
            if read_cursor and not isinstance(read_cursor, CommandCursor):
                read_cursor = read_cursor.hint(index_hint)

        if read_result:
            return read_result

        return read_cursor

    def bulk_commit(self):
        if not self._ops:
            return None

        result = self.collection.bulk_write(self._ops)
        self._ops.clear()
        return result

    def bulk_create(self, *, data: Dict, actor: str = "yali"):
        self.sync_created_audit(doc=data, actor=actor)
        self.sync_updated_audit(doc=data, actor=actor)

        self._ops.append(InsertOne(data))

        if len(self._ops) >= _docdb_settings.db_max_bulk_ops:
            return self.bulk_commit()

        return None

    def bulk_update(
        self,
        *,
        filters: Dict,
        data: Dict,
        upsert: bool = False,
        update_one: bool = True,
        actor: str = "yali",
    ):
        creation_audit = {}
        updation_audit = {}

        if OP_SET in data:
            self.sync_updated_audit(doc=data[OP_SET], actor=actor)
        else:
            self.sync_updated_audit(doc=updation_audit, actor=actor)
            data.update({OP_SET: updation_audit})

        if OP_SET_ON_INSERT in data:
            self.sync_created_audit(doc=data[OP_SET_ON_INSERT], actor=actor)
        else:
            self.sync_created_audit(doc=creation_audit, actor=actor)
            data.update({OP_SET_ON_INSERT: creation_audit})

        if update_one:
            self._ops.append(UpdateOne(filter=filters, update=data, upsert=upsert))
        else:
            self._ops.append(UpdateMany(filter=filters, update=data, upsert=upsert))

        if len(self._ops) >= _docdb_settings.db_max_bulk_ops:
            return self.bulk_commit()

        return None

    def bulk_save(self, *, docs: List[Dict], actor: str = "yali"):
        for doc in docs:
            doc_id = doc["_id"]
            cached_doc = self.record_by_id(doc_id)

            if cached_doc:
                cached_doc.update(doc)
                self.sync_updated_audit(doc=cached_doc, actor=actor)
                self._ops.append(UpdateOne({"_id": doc_id}, {OP_SET: cached_doc}))
            else:
                self.sync_created_audit(doc=doc, actor=actor)
                self.sync_updated_audit(doc=doc, actor=actor)
                self._ops.append(InsertOne(doc))

        if len(self._ops) >= _docdb_settings.db_max_bulk_ops:
            return self.bulk_commit()

        return None

    def record_count(self, *, filters: Dict = {}):
        return self.collection.count_documents(filter=filters)

    @cache_func.ttl_cache(
        maxsize=_docdb_settings.db_cache_size, ttl=_docdb_settings.db_cache_ttl
    )
    def record_by_id(self, doc_id: DocId) -> Dict | None:
        return self.collection.find_one({"_id": doc_id})
