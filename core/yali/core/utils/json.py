from typing import Any, Callable, Dict

import orjson

from ..typings import JsonType, StrictTypesModel

EncodeFunc = Callable[[Any], Any]


class EncodeOptions(StrictTypesModel):
    prettify: bool = False
    sort_keys: bool = False
    omit_microseconds: bool = False
    customize_dataclass: bool = False
    customize_datetime: bool = False
    customize_subclass: bool = False


class JsonConv:
    @staticmethod
    def is_valid(data: Any):
        """Check if the data is a valid JSON."""
        return isinstance(data, (dict, list))

    @staticmethod
    def safe_load(data: str):
        """Load JSON data safely."""
        try:
            payload = orjson.loads(data)
            assert isinstance(payload, (dict, list))
            return payload
        except orjson.JSONDecodeError:
            return data

    @staticmethod
    def dump_to_bytes(
        data: Dict,
        *,
        options: EncodeOptions = EncodeOptions(),
        custom_encoder: EncodeFunc | None = None,
    ):
        dump_opts = orjson.OPT_SERIALIZE_NUMPY

        if options.prettify:
            dump_opts |= orjson.OPT_INDENT_2

        if options.sort_keys:
            dump_opts |= orjson.OPT_SORT_KEYS

        if options.omit_microseconds:
            dump_opts |= orjson.OPT_OMIT_MICROSECONDS

        if not custom_encoder:
            return orjson.dumps(data, option=dump_opts)

        if options.customize_dataclass:
            dump_opts |= orjson.OPT_PASSTHROUGH_DATACLASS
        elif options.customize_datetime:
            dump_opts |= orjson.OPT_PASSTHROUGH_DATETIME
        elif options.customize_subclass:
            dump_opts |= orjson.OPT_PASSTHROUGH_SUBCLASS

        return orjson.dumps(data, option=dump_opts, default=custom_encoder)

    @staticmethod
    def dump_to_str(
        data: Dict,
        *,
        options: EncodeOptions = EncodeOptions(),
        custom_encoder: EncodeFunc | None = None,
    ):
        return JsonConv.dump_to_bytes(
            data, options=options, custom_encoder=custom_encoder
        ).decode("utf-8")

    @staticmethod
    def dump_to_file(
        data: str,
        *,
        file_path: str,
        options: EncodeOptions = EncodeOptions(),
        custom_encoder: EncodeFunc | None = None,
    ):
        json_str = JsonConv.dump_to_bytes(
            data, options=options, custom_encoder=custom_encoder
        ).decode("utf-8")

        with open(file_path, "w") as f:
            f.write(json_str)

    @staticmethod
    def load_from_str(data: str) -> JsonType:
        return orjson.loads(data)

    @staticmethod
    def load_from_file(file_path: str) -> JsonType:
        with open(file_path, "r") as f:
            return orjson.loads(f.read())
