from typing import Annotated

import msgspec
import pytest

import yali.core.typebase as tb


def test_unsigned_int():
    msgspec.json.decode(b"0", type=tb.UnsignedInt)
    msgspec.json.decode(b"1000", type=tb.UnsignedInt)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b"-1", type=tb.UnsignedInt)

    assert str(ex.value) == "Expected `int` >= 0"


def test_positive_int():
    msgspec.json.decode(b"1", type=tb.PositiveInt)
    msgspec.json.decode(b"1000", type=tb.PositiveInt)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b"0", type=tb.PositiveInt)

    assert str(ex.value) == "Expected `int` >= 1"


def test_tz_date_time():
    msgspec.json.decode(b'"2023-01-01T00:00:00+05:30"', type=tb.TZDateTime)
    msgspec.json.decode(b'"2023-01-01T00:00:00Z"', type=tb.TZDateTime)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"1490356924852"', type=tb.TZDateTime)

    assert str(ex.value) == "Invalid RFC3339 encoded datetime"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"2023-01-01T00:00:00"', type=tb.TZDateTime)

    assert str(ex.value) == "Expected `datetime` with a timezone component"


def test_non_tz_date_time():
    msgspec.json.decode(b'"2023-01-01T00:00:00"', type=tb.NonTZDateTime)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"1490356924852"', type=tb.TZDateTime)

    assert str(ex.value) == "Invalid RFC3339 encoded datetime"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"2023-01-01T00:00:00+05:30"', type=tb.NonTZDateTime)

    assert str(ex.value) == "Expected `datetime` with no timezone component"


def test_tz_time():
    msgspec.json.decode(b'"00:00:00+05:30"', type=tb.TZTime)
    msgspec.json.decode(b'"00:00:00Z"', type=tb.TZTime)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"1490356924852"', type=tb.TZTime)

    assert str(ex.value) == "Invalid RFC3339 encoded time"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"00:00:00"', type=tb.TZTime)

    assert str(ex.value) == "Expected `time` with a timezone component"


def test_non_tz_time():
    msgspec.json.decode(b'"00:00:00"', type=tb.NonTZTime)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"1490356924852"', type=tb.TZTime)

    assert str(ex.value) == "Invalid RFC3339 encoded time"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"00:00:00+05:30"', type=tb.NonTZTime)

    assert str(ex.value) == "Expected `time` with no timezone component"


def test_empty_str():
    msgspec.json.decode(b'""', type=tb.EmptyStr)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"abc"', type=tb.EmptyStr)

    assert str(ex.value) == "Expected `str` of length <= 0"


def test_non_empty_str():
    msgspec.json.decode(b'"abc"', type=tb.NonEmptyStr)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'""', type=tb.NonEmptyStr)

    assert str(ex.value) == "Expected `str` of length >= 1"


def test_snake_case_str():
    msgspec.json.decode(b'"abc_def"', type=tb.SnakeCaseStr)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"abc-def"', type=tb.SnakeCaseStr)

    assert str(ex.value) == "Expected `str` matching regex '^[a-z0-9_]+$'"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"aB1-d$f"', type=tb.SnakeCaseStr)

    assert str(ex.value) == "Expected `str` matching regex '^[a-z0-9_]+$'"


def test_amqp_url():
    msgspec.json.decode(b'"amqp://guest:guest@localhost:5672/guest"', type=tb.AmqpUrl)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"amqp://guest:guest@localhost:5672"', type=tb.AmqpUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:amqp|amqps)://(\\\\w+):(\\\\w+)@(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"amqp://guest:guest@localhost/guest"', type=tb.AmqpUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:amqp|amqps)://(\\\\w+):(\\\\w+)@(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )


def test_websocket_url():
    msgspec.json.decode(b'"ws://localhost:5672/guest"', type=tb.WebsocketUrl)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"ws://localhost:5672"', type=tb.WebsocketUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:ws|wss)://(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"ws://localhost/guest"', type=tb.WebsocketUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:ws|wss)://(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )


def test_mongo_url():
    msgspec.json.decode(
        b'"mongodb://user:pass@localhost:27017/guest"', type=tb.MongoUrl
    )

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"mongodb://localhost"', type=tb.MongoUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:mongodb|mongodb+srv)://(\\\\w+):(\\\\w+)@(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"mongodb://user:pass@localhost:27017"', type=tb.MongoUrl)

    assert (
        str(ex.value)
        == "Expected `str` matching regex '^(?:mongodb|mongodb+srv)://(\\\\w+):(\\\\w+)@(\\\\w+):(\\\\d+)/(\\\\w+)$'"
    )


def test_secret_str():
    mypass: tb.SecretStr = tb.SecretStr("abcd!123")

    assert mypass.get_secret_value() == "abcd!123"
    assert str(mypass) == "********"
    assert repr(mypass) == "SecretStr('********')"


def test_constr_node():
    MultiOf2 = Annotated[int, tb.ConstrNode.constr_num(multiple_of=2, ge=0)]
    Str8 = Annotated[str, tb.ConstrNode.constr_str(min_length=8, max_length=8)]
    KebabStr = Annotated[
        str, tb.ConstrNode.constr_str(pattern=r"^[a-z0-9-]+$", min_length=1)
    ]
    IntList8 = Annotated[
        list[int], tb.ConstrNode.constr_seq(min_length=8, max_length=8)
    ]

    msgspec.json.decode(b"0", type=MultiOf2)
    msgspec.json.decode(b"2", type=MultiOf2)
    msgspec.json.decode(b"4", type=MultiOf2)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b"3", type=MultiOf2)

    assert str(ex.value) == "Expected `int` that's a multiple of 2"

    msgspec.json.decode(b'"abcd1234"', type=Str8)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"abcd123"', type=Str8)

    assert str(ex.value) == "Expected `str` of length >= 8"

    msgspec.json.decode(b'"abcd1234"', type=KebabStr)
    msgspec.json.decode(b'"abcd-1234"', type=KebabStr)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'"abcd_123"', type=KebabStr)

    assert str(ex.value) == "Expected `str` matching regex '^[a-z0-9-]+$'"

    msgspec.json.decode(b"[1, 2, 3, 4, 5, 6, 7, 8]", type=IntList8)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b"[1, 2, 3, 4, 5, 6, 7]", type=IntList8)

    assert str(ex.value) == "Expected `array` of length >= 8"

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(b'[1, 2, 3, 4, 5, 6, 7, "abc"]', type=IntList8)

    assert str(ex.value) == "Expected `int`, got `str` - at `$[7]`"

    with pytest.raises(msgspec.DecodeError) as ex:
        msgspec.json.decode(b"[1, 2, 3, 4, 5, 6, True, 8]", type=IntList8)

    assert str(ex.value).startswith("JSON is malformed: invalid character (byte")
