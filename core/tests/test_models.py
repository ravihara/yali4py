import datetime as dt

import msgspec
import pytest

import yali.core.models as mdl
from yali.core.codecs import JSONNode


def test_base_model_fields():
    model = mdl.BaseModel()
    assert model.__struct_fields__ == ()

    class TestModel(mdl.BaseModel):
        a: int
        b: str
        c: float

    model = TestModel(a=1, b="test", c=1.0)
    assert model.__struct_fields__ == ("a", "b", "c")


def test_aio_except_value():
    model = mdl.AioExceptValue(name="test", exc_info=Exception("test"))
    assert model.name == "test"
    assert str(model.exc_info) == "test"
    assert model.extra == "Ting"

    assert len(model.__struct_fields__) == 3

    model = mdl.AioExceptValue(name="test", exc_info=Exception("test"), extra=1)
    mval = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)

    with pytest.raises(msgspec.ValidationError) as ex:
        msgspec.json.decode(
            mval, type=mdl.AioExceptValue, dec_hook=JSONNode.default_dec_hook
        )

    assert str(ex.value) == "Expected `str | null`, got `int` - at `$.extra`"


def test_date_time_range():
    start_dt = dt.datetime(1998, 10, 1)
    end_dt = dt.datetime(1998, 10, 2)

    model = mdl.DateTimeRange(start_dt=start_dt, end_dt=end_dt)

    assert model.start_dt == start_dt
    assert model.end_dt == end_dt

    start_dt = dt.datetime(1998, 10, 2)
    end_dt = dt.datetime(1998, 10, 1)

    with pytest.raises(ValueError) as excinfo:
        mdl.DateTimeRange(start_dt=start_dt, end_dt=end_dt)

    assert (
        str(excinfo.value)
        == f"Value of end_dt '{end_dt}' should be greater than or equal to start_dt '{start_dt}'"
    )


def test_failure():
    model = mdl.Failure(
        error="test",
        cause=Exception("test"),
        extra={"test": "test"},
    )

    assert model.error == "test"
    assert str(model.cause) == "test"
    assert model.extra == {"test": "test"}

    assert len(model.__struct_fields__) == 3

    model = mdl.Failure(error="another test")

    assert model.error == "another test"
    assert model.cause is None
    assert model.extra is None

    assert len(model.__struct_fields__) == 3

    mval = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)
    msgspec.json.decode(mval, type=mdl.Failure, dec_hook=JSONNode.default_dec_hook)


def test_success():
    model = mdl.Success(data={"key1": "value1", "key2": "value2"})

    assert model.data == {"key1": "value1", "key2": "value2"}
    assert model.code is None

    assert len(model.__struct_fields__) == 2

    model = mdl.Success(data={"key1": "value1", "key2": "value2"}, code=1)

    assert model.data == {"key1": "value1", "key2": "value2"}
    assert model.code == 1

    assert len(model.__struct_fields__) == 2

    mval = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)
    msgspec.json.decode(mval, type=mdl.Success, dec_hook=JSONNode.default_dec_hook)

    model = mdl.Success(data={"key1": "value1", "key2": "value2"}, code=-1)

    with pytest.raises(msgspec.ValidationError) as excinfo:
        val = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)
        msgspec.json.decode(val, type=mdl.Success, dec_hook=JSONNode.default_dec_hook)

    assert str(excinfo.value) == "Expected `int` >= 0 - at `$.code`"


def test_multi_result():
    model = mdl.MultiResult()

    assert model.passed == []
    assert model.failed == []
    assert model.summary is None

    assert len(model.__struct_fields__) == 3

    model = mdl.MultiResult(
        passed=[{"key1": "value1", "key2": "value2"}],
        failed=[mdl.Failure(error="test", cause=Exception("test"))],
        summary="Just a test",
    )

    assert model.passed == [{"key1": "value1", "key2": "value2"}]
    assert isinstance(model.failed[0], mdl.Failure)
    assert model.summary == "Just a test"

    assert len(model.__struct_fields__) == 3

    mval = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)
    msgspec.json.decode(mval, type=mdl.MultiResult, dec_hook=JSONNode.default_dec_hook)

    model = mdl.MultiResult(
        passed=[{"key1": "value1", "key2": "value2"}],
        failed=[mdl.Failure(error="test", cause=Exception("test"))],
        summary=1,
    )

    with pytest.raises(msgspec.ValidationError) as excinfo:
        val = msgspec.json.encode(model, enc_hook=JSONNode.default_enc_hook)
        msgspec.json.decode(
            val, type=mdl.MultiResult, dec_hook=JSONNode.default_dec_hook
        )

    assert str(excinfo.value) == "Expected `str | null`, got `int` - at `$.summary`"


def test_dyn_data_model():
    mdl01_options = mdl.DataModelOptions()
    model_klass = mdl.data_model(
        name="Model01",
        fields=[
            ("name", "str"),
            ("age", "int"),
            ("score", "float", mdl.field_specs(default=1.0)),
        ],
        options=mdl01_options,
    )

    assert isinstance(model_klass, type(msgspec.Struct))

    mdl1 = model_klass(name="test", age=1, score=2.0)

    assert mdl1.name == "test"
    assert mdl1.age == 1
    assert type(mdl1.score) is float
    assert mdl1.__struct_config__.tag == "YaliMdl.Model01"
    assert len(mdl1.__struct_fields__) == 3

    mval = msgspec.json.encode(mdl1, enc_hook=JSONNode.default_enc_hook)
    msgspec.json.decode(mval, type=model_klass, dec_hook=JSONNode.default_dec_hook)

    mdl2 = model_klass(name="test", age=1)

    assert mdl2.name == "test"
    assert mdl2.age == 1
    assert mdl2.score >= 1.0

    with pytest.raises(TypeError) as ex:
        model_klass(name="test", age=1, score=2.0, extra=False)

    assert str(ex.value) == "Unexpected keyword argument 'extra'"
