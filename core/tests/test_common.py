import os
import sys

import yali.core.common as common
from yali.core.models import Failure, Success


def test_mproc_spawn_context():
    common.yali_main_init()
    assert common.yali_mproc_context()._name == "spawn"

    os.environ["MULTI_PROCESS_CONTEXT"] = "fork"
    common.yali_main_init()
    assert common.yali_mproc_context()._name == "spawn"


def test_os_uname_str():
    ustr = common.os_uname_str()
    assert len(ustr.split("|")) == 5


def test_alphanum_sorted():
    in_list = ["Ravi", "Kavi", "Savi", "Shiva", "Shyam"]
    out_list = ["Kavi", "Ravi", "Savi", "Shiva", "Shyam"]

    assert common.alphanum_sorted(in_list) == out_list


def test_sizeof_object():
    obj = {
        "a1": "Hi",
        "b1": 2000,
        "c1": {"ca1": 300, "cb1": "Hello", "cc1": [1, 2, 3, 4, 5]},
    }
    osize = common.sizeof_object(obj)

    assert osize > sys.getsizeof(obj)


def test_get_sys_ipaddrs():
    ipaddrs = common.get_sys_ipaddrs()
    assert len(ipaddrs) > 0


def test_sysinfo_vals():
    val = common.id_by_sysinfo(suffix="test", use_pid=True, hash_algo="sha256")
    assert val.endswith("test")

    fname = common.filename_by_sysinfo(basename="test", extension=".log")
    assert fname.startswith("test_")
    assert fname.endswith(".log")
    assert len(fname) > 9


def test_dict_to_result():
    d = {"key1": "value1", "key2": "value2"}
    r = common.dict_to_result(data={"data": d})
    assert isinstance(r, Success)
    assert r.data == d

    r = common.dict_to_result(data={"error": "test", "extra": d})
    assert isinstance(r, Failure)
    assert r.error == "test"
    assert r.extra == d

    r = common.dict_to_result(data=d)
    assert isinstance(r, Failure)
    assert r.error == "Invalid data"
    assert r.extra == {"data": d}
