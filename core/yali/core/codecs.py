from typing import Any, Type

import msgspec

from .typebase import DataType, SecretStr


class JSONNode:
    @staticmethod
    def default_enc_hook(obj: Any) -> Any:
        if isinstance(obj, SecretStr):
            return obj.get_secret_value()
        elif isinstance(obj, complex):
            return (obj.real, obj.imag)
        elif isinstance(obj, BaseException):
            return str(obj.args[0]) if obj.args else obj.__str__()

        raise NotImplementedError(f"Objects of type {type(obj)} are not supported")

    @staticmethod
    def default_dec_hook(type: Type, obj: Any) -> Any:
        if type is SecretStr:
            return SecretStr(obj)
        elif type is complex:
            return complex(obj[0], obj[1])
        elif issubclass(type, BaseException):
            return type(obj)

        raise NotImplementedError(f"Objects of type {type} are not supported")

    @staticmethod
    def is_valid_type(data: Any):
        return isinstance(data, (dict, list))

    ## Encoding functions
    @staticmethod
    def dump_bytes(data: Any):
        return msgspec.json.encode(data, enc_hook=JSONNode.default_enc_hook)

    @staticmethod
    def dump_str(data: Any):
        return msgspec.json.encode(data, enc_hook=JSONNode.default_enc_hook).decode(
            "utf-8"
        )

    @staticmethod
    def dump_file(data: Any, *, file_path: str):
        with open(file_path, "w") as f:
            return f.write(
                msgspec.json.encode(data, enc_hook=JSONNode.default_enc_hook).decode(
                    "utf-8"
                )
            )

    ## Decoding functions
    @staticmethod
    def load_data(data: bytes | str, *, dec_type: DataType | None = None):
        if data is None:
            return None

        if dec_type:
            return msgspec.json.decode(
                data, type=dec_type, dec_hook=JSONNode.default_dec_hook
            )

        return msgspec.json.decode(data, dec_hook=JSONNode.default_dec_hook)

    @staticmethod
    def load_file(file_path: str, *, dec_type: DataType | None = None):
        with open(file_path, "rb") as f:
            if dec_type:
                return msgspec.json.decode(
                    f.read(), type=dec_type, dec_hook=JSONNode.default_dec_hook
                )

            return msgspec.json.decode(f.read(), dec_hook=JSONNode.default_dec_hook)

    @staticmethod
    def safe_load_data(data: bytes | str, *, dec_type: DataType | None = None):
        try:
            if dec_type:
                payload = msgspec.json.decode(
                    data, type=dec_type, dec_hook=JSONNode.default_dec_hook
                )
            else:
                payload = msgspec.json.decode(data, dec_hook=JSONNode.default_dec_hook)

            assert isinstance(payload, (dict, list))
            return payload
        except (msgspec.ValidationError, msgspec.DecodeError):
            ## In case of exception, return the original data
            return data

    @staticmethod
    def codecs_for_type(data_type: DataType):
        encoder = msgspec.json.Encoder(enc_hook=JSONNode.default_enc_hook)
        decoder = msgspec.json.Decoder(
            type=data_type, dec_hook=JSONNode.default_dec_hook
        )

        return encoder, decoder
