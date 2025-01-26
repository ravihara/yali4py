import os

import msgspec
import pytest

from yali.core.codecs import JSONNode
from yali.core.models import BaseModel
from yali.core.typebase import SecretStr

sample01 = {
    "strkey1": "value1",
    "intkey2": 2,
    "boolkey3": True,
}

sample02 = {
    "strkey1": "value1",
    "intkey2": 2,
    "cpxkey3": complex(1, 2),
    "exckey4": Exception("test"),
    "seckey5": SecretStr("my-super-secret"),
    "boolkey6": True,
}


class MyModel(BaseModel):
    strkey1: str
    intkey2: int
    cpxkey3: complex
    exckey4: Exception
    seckey5: SecretStr
    boolkey6: bool


@pytest.fixture(scope="module")
def test_json_file():
    yield "test.json"

    try:
        os.remove("test.json")
    except FileNotFoundError:
        pass
    except PermissionError:
        print("Permission denied to remove test.json")


def test_str_ops():
    assert JSONNode.is_valid_type(data=sample01)
    assert JSONNode.is_valid_type(data=sample02)

    json_str = JSONNode.dump_str(sample01)
    json_data = JSONNode.load_data(data=json_str)
    assert json_data == sample01

    json_str = JSONNode.dump_str(sample02)
    json_data = JSONNode.load_data(data=json_str)

    assert json_data.get("seckey5") == "my-super-secret"


def test_bytes_ops():
    json_bytes = JSONNode.dump_bytes(sample01)
    json_data = JSONNode.load_data(data=json_bytes)
    assert json_data == sample01

    json_bytes = JSONNode.dump_bytes(sample02)
    json_data = JSONNode.load_data(data=json_bytes)

    assert json_data.get("seckey5") == "my-super-secret"


def test_struct_ops():
    my_model1 = MyModel(**sample02)

    json_bytes = JSONNode.dump_bytes(my_model1)
    json_data = JSONNode.load_data(data=json_bytes, dec_type=MyModel)

    assert isinstance(json_data, MyModel)
    assert my_model1.strkey1 == json_data.strkey1
    assert my_model1.intkey2 == json_data.intkey2
    assert my_model1.cpxkey3 == json_data.cpxkey3
    assert str(my_model1.exckey4) == str(json_data.exckey4)
    assert my_model1.seckey5 == json_data.seckey5
    assert my_model1.boolkey6 == json_data.boolkey6


def test_file_ops(test_json_file):
    my_model1 = MyModel(**sample02)

    JSONNode.dump_file(my_model1, file_path=test_json_file)
    json_data = JSONNode.load_file(file_path=test_json_file, dec_type=MyModel)

    assert isinstance(json_data, MyModel)
    assert my_model1.strkey1 == json_data.strkey1
    assert my_model1.intkey2 == json_data.intkey2
    assert my_model1.cpxkey3 == json_data.cpxkey3
    assert str(my_model1.exckey4) == str(json_data.exckey4)
    assert my_model1.seckey5 == json_data.seckey5
    assert my_model1.boolkey6 == json_data.boolkey6


def test_safe_load_data():
    json_str = '{"_yali_mdl_tag":"YaliMdl.MyModel","strkey1":"value1","intkey2":2,"cpxkey3":[1.0,2.0],"exckey4":"test","seckey5":"my-super-secret","boolkey6":true}'

    assert JSONNode.safe_load_data(data="test") == "test"
    assert isinstance(JSONNode.safe_load_data(data=json_str), dict)
    assert JSONNode.safe_load_data(data="test", dec_type=MyModel) == "test"


def test_codecs_for_type():
    encoder, decoder = JSONNode.codecs_for_type(data_type=MyModel)

    assert isinstance(encoder, msgspec.json.Encoder)
    assert isinstance(decoder, msgspec.json.Decoder)

    my_model1 = MyModel(**sample02)

    json_bytes = encoder.encode(my_model1)
    json_data = decoder.decode(json_bytes)

    assert isinstance(json_data, MyModel)
    assert my_model1.strkey1 == json_data.strkey1
    assert my_model1.intkey2 == json_data.intkey2
    assert my_model1.cpxkey3 == json_data.cpxkey3
    assert str(my_model1.exckey4) == str(json_data.exckey4)
    assert my_model1.seckey5 == json_data.seckey5
    assert my_model1.boolkey6 == json_data.boolkey6
