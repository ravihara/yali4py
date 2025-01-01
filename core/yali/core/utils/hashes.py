import hashlib
import re
from typing import Callable, Dict

import orjson


class Hasher:
    __md5_hash_regex = re.compile(r"^[a-fA-F0-9]{32}$")
    __sha256_hash_regex = re.compile(r"^[a-fA-F0-9]{64}$")

    @staticmethod
    def is_formatted_md5(hash_val: str) -> bool:
        """Check if the hash value is a valid MD5 hash."""
        return bool(Hasher.__md5_hash_regex.match(hash_val))

    @staticmethod
    def is_formatted_sha256(hash_val: str) -> bool:
        """Check if the hash value is a valid SHA256 hash."""
        return bool(Hasher.__sha256_hash_regex.match(hash_val))

    @staticmethod
    def generate_md5_hash(
        payload: Dict,
        *,
        encoder_fn: Callable | None = None,
    ) -> str:
        """
        Generate a MD5 hash from the given payload

        Parameters
        ----------
        payload : Dict
            The payload to be hashed
        encoder_fn : Callable | None, optional
            The encoder function to be used, by default None

        Returns
        -------
        str
            The MD5 hash
        """
        bytes_val = orjson.dumps(payload, default=encoder_fn)
        hash_val = hashlib.md5(bytes_val).hexdigest()

        return hash_val

    @staticmethod
    def generate_sha256_hash(
        payload: Dict, *, encoder_fn: Callable | None = None
    ) -> str:
        """
        Generate a SHA256 hash from the given payload

        Parameters
        ----------
        payload : Dict
            The payload to be hashed
        encoder_fn : Callable | None, optional
            The encoder function to be used, by default None

        Returns
        -------
        str
            The SHA256 hash
        """
        bytes_val = orjson.dumps(payload, default=encoder_fn)
        hash_val = hashlib.sha256(bytes_val).hexdigest()

        return hash_val
