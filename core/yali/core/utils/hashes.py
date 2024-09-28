import hashlib
import json
import re
from json import JSONEncoder
from typing import Callable, Dict


class Hasher:
    __md5_hash_regex = re.compile(r"^[a-fA-F0-9]{32}$")
    __sha256_hash_regex = re.compile(r"^[a-fA-F0-9]{64}$")

    @staticmethod
    def is_formatted_md5(hash_val: str) -> bool:
        return bool(Hasher.__md5_hash_regex.match(hash_val))

    @staticmethod
    def is_formatted_sha256(hash_val: str) -> bool:
        return bool(Hasher.__sha256_hash_regex.match(hash_val))

    @staticmethod
    def generate_md5_hash(
        payload: Dict,
        *,
        encoder_fn: Callable | None = None,
        encoder_cls: type[JSONEncoder] | None = None,
    ) -> str:
        bytes_val = json.dumps(payload, default=encoder_fn, cls=encoder_cls)
        hash_val = hashlib.md5(bytes_val.encode()).hexdigest()

        return hash_val

    @staticmethod
    def generate_sha256_hash(
        payload: Dict,
        *,
        encoder_fn: Callable | None = None,
        encoder_cls: type[JSONEncoder] | None = None,
    ) -> str:
        bytes_val = json.dumps(payload, default=encoder_fn, cls=encoder_cls)
        hash_val = hashlib.sha256(bytes_val.encode()).hexdigest()

        return hash_val
