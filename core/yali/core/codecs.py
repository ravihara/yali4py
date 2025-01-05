from typing import Any, Callable, Type, TypeVar

import msgspec

YaliT = TypeVar("YaliT")
DataType = Type[YaliT]

JSONEncHook = Callable[[Any], Any]
JSONDecHook = Callable[[Type, Any], Any]
JSONEncoder = msgspec.json.Encoder
JSONDecoder = msgspec.json.Decoder


## Default encoding hook
def json_default_enc_hook(obj: Any) -> Any:
    if isinstance(obj, BaseException):
        return str(obj.args[0]) if obj.args else obj.__str__()
    elif isinstance(obj, complex):
        return (obj.real, obj.imag)

    raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


## Default decoding hook
def json_default_dec_hook(type: Type, obj: Any) -> Any:
    if issubclass(type, BaseException):
        return type(obj)
    elif type is complex:
        return complex(obj[0], obj[1])

    raise NotImplementedError(f"Objects of type {type} are not supported")


def is_valid_json(data: Any):
    return isinstance(data, (dict, list))


## Encoding functions
def to_json_data(data: Any, *, as_string: bool = False):
    result = msgspec.json.encode(data, enc_hook=json_default_enc_hook)

    if as_string:
        return result.decode("utf-8")

    return result


def to_json_file(data: Any, *, file_path: str):
    with open(file_path, "w") as f:
        return f.write(
            msgspec.json.encode(data, enc_hook=json_default_enc_hook).decode("utf-8")
        )


## Decoding functions
def from_json_data(data: bytes | str, *, dec_type: DataType | None = None):
    if dec_type:
        return msgspec.json.decode(data, type=dec_type, dec_hook=json_default_dec_hook)

    return msgspec.json.decode(data, dec_hook=json_default_dec_hook)


def from_json_file(file_path: str, *, dec_type: DataType | None = None):
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
    except msgspec.ValidationError:
        ## In case of exception, return the original data
        return data


def json_codecs(data_type: DataType):
    encoder = msgspec.json.Encoder(enc_hook=json_default_enc_hook)
    decoder = msgspec.json.Decoder(type=data_type, dec_hook=json_default_dec_hook)

    return encoder, decoder
