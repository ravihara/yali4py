import datetime as dt

import pytest
from msgspec import ValidationError

from yali.core.models import BaseModel, DateTimeRange


def test_flexi_types_model():
    model = BaseModel()
    assert model.__struct_fields__ == {}

    class TestModel(BaseModel):
        a: int
        b: str
        c: float

    model = TestModel(a=1, b="test", c=1.0)
    assert model.__struct_fields__ == {"a", "b", "c"}


def test_date_time_range():
    start_dt = dt.datetime(1998, 10, 1)
    end_dt = dt.datetime(1998, 10, 2)
    model = DateTimeRange(start_dt=start_dt, end_dt=end_dt)
    assert model.start_dt == start_dt
    assert model.end_dt == end_dt

    with pytest.raises(ValidationError) as excinfo:
        DateTimeRange(start_dt=end_dt, end_dt=start_dt)

    assert (
        str(excinfo.value)
        == "Value error, 1998-10-01 00:00:00 < 1998-10-02 00:00:00 - Value of 'end_dt' should be greater than or equal to 'start_dt'"
    )
