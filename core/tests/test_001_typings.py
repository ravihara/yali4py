import datetime as dt

import pytest
from pydantic import ValidationError
from yali.core.typings import DateTimeRange, StrictTypesModel, FlexiTypesModel, SkipTypesModel


def test_flexi_types_model():
    model = FlexiTypesModel()
    assert model.extra_fields == set()

    class TestModel(FlexiTypesModel):
        a: int
        b: str
        c: float

    model = TestModel(a=1, b="test", c=1.0, d="extra")
    assert model.extra_fields == {"d"}
    assert model.extra_properties == {"d": "extra"}


def test_strict_types_model():
    model = StrictTypesModel()
    assert hasattr(model, "extra_fields") is False
    assert hasattr(model, "extra_properties") is False

    class TestModel(StrictTypesModel):
        a: int
        b: str
        c: float

    with pytest.raises(ValidationError) as excinfo:
        TestModel(a=1, b="test", c=1.0, d="extra")

    errors = excinfo.value.errors()
    assert len(errors) == 1

    assert errors[0]["type"] == "extra_forbidden"
    assert errors[0]["msg"] == "Extra inputs are not permitted"
    assert errors[0]["input"] == "extra"


def test_skip_types_model():
    model = SkipTypesModel()
    assert hasattr(model, "extra_fields") is False
    assert hasattr(model, "extra_properties") is False

    class TestModel(SkipTypesModel):
        a: int
        b: str
        c: float

    model = TestModel(a=1, b="test", c=1.0, d="extra")
    assert len(model.model_fields) == 3
    assert hasattr(model, "d") is False


def test_date_time_range():
    start_dt = dt.datetime(1998, 10, 1)
    end_dt = dt.datetime(1998, 10, 2)
    model = DateTimeRange(start_dt=start_dt, end_dt=end_dt)
    assert model.start_dt == start_dt
    assert model.end_dt == end_dt

    with pytest.raises(ValidationError) as excinfo:
        DateTimeRange(start_dt=end_dt, end_dt=start_dt)

    errors = excinfo.value.errors()
    assert len(errors) == 1

    assert errors[0]["type"] == "value_error"
    assert (
        errors[0]["msg"]
        == "Value error, 1998-10-01 00:00:00 < 1998-10-02 00:00:00 - Value of 'end_dt' should be greater than or equal to 'start_dt'"
    )
