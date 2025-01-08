from typing import Any

import msgspec

from .hooks import json_default_dec_hook, json_default_enc_hook
from .metatypes import DataType


def is_valid_json(data: Any):
    return isinstance(data, (dict, list))


## Encoding functions
def data_to_json(data: Any, *, as_string: bool = False):
    result = msgspec.json.encode(data, enc_hook=json_default_enc_hook)

    if as_string:
        return result.decode("utf-8")

    return result


def data_to_json_file(data: Any, *, file_path: str):
    with open(file_path, "w") as f:
        return f.write(
            msgspec.json.encode(data, enc_hook=json_default_enc_hook).decode("utf-8")
        )


## Decoding functions
def data_from_json(data: bytes | str, *, dec_type: DataType | None = None):
    if data is None:
        return None

    if dec_type:
        return msgspec.json.decode(data, type=dec_type, dec_hook=json_default_dec_hook)

    return msgspec.json.decode(data, dec_hook=json_default_dec_hook)


def data_from_json_file(file_path: str, *, dec_type: DataType | None = None):
    with open(file_path, "rb") as f:
        if dec_type:
            return msgspec.json.decode(
                f.read(), type=dec_type, dec_hook=json_default_dec_hook
            )

        return msgspec.json.decode(f.read(), dec_hook=json_default_dec_hook)


def safe_load_json(data: bytes | str, *, dec_type: DataType | None = None):
    try:
        if dec_type:
            payload = msgspec.json.decode(
                data, type=dec_type, dec_hook=json_default_dec_hook
            )
        else:
            payload = msgspec.json.decode(data, dec_hook=json_default_dec_hook)

        assert isinstance(payload, (dict, list))
        return payload
    except (msgspec.ValidationError, msgspec.DecodeError):
        ## In case of exception, return the original data
        return data


def json_codecs(data_type: DataType):
    encoder = msgspec.json.Encoder(enc_hook=json_default_enc_hook)
    decoder = msgspec.json.Decoder(type=data_type, dec_hook=json_default_dec_hook)

    return encoder, decoder
