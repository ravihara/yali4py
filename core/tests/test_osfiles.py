from os import path as ospath
from typing import List

from yali.core.osfiles import FSNode

base_dir = ospath.dirname(__file__)
data_dir = ospath.join(base_dir, "samples", "data")


def test_is_file_readable():
    assert FSNode.is_file_readable(f"{base_dir}/samples/testfile.txt") is True
    assert FSNode.is_file_readable(f"{base_dir}/samples/nofile.txt") is False


def test_is_file_writable():
    assert FSNode.is_file_writable(f"{base_dir}/samples/testfile.txt") is True
    assert (
        FSNode.is_file_writable(
            f"{base_dir}/samples/testfile1.txt", check_creatable=True
        )
        is True
    )
    assert (
        FSNode.is_file_writable(
            f"{base_dir}/samples/nodir/testfile.txt", check_creatable=True
        )
        is False
    )


def test_is_dir_readable():
    assert FSNode.is_dir_readable(f"{base_dir}/samples") is True
    assert FSNode.is_dir_readable(f"{base_dir}/samples/nodir") is False


def test_is_dir_writable():
    assert FSNode.is_dir_writable(f"{base_dir}/samples") is True
    assert (
        FSNode.is_dir_writable(f"{base_dir}/samples/newdir", check_creatable=True)
        is True
    )
    assert (
        FSNode.is_dir_writable(f"{base_dir}/samples/nodir/newdir", check_creatable=True)
        is False
    )


def test_file_paths():
    fpaths: List[str] = []

    for fpath in FSNode.file_paths_from_dir(base_dir=data_dir, extensions=[".rc"]):
        fpaths.append(fpath)

    assert len(fpaths) == 1
    assert fpaths[0] == ospath.join(data_dir, "sample.rc")

    fpaths.clear()

    for fpath in FSNode.file_paths_from_dir(
        base_dir=data_dir, extensions=[".in", ".txt"]
    ):
        fpaths.append(fpath)

    assert len(fpaths) == 8
    assert ospath.join(data_dir, "sample.in") in fpaths
    assert ospath.join(data_dir, "sample.txt") in fpaths
    assert ospath.join(data_dir, "d1/d1.in") in fpaths
    assert ospath.join(data_dir, "d1/d1.txt") in fpaths
    assert ospath.join(data_dir, "d2/d2.in") in fpaths
    assert ospath.join(data_dir, "d2/d2.txt") in fpaths
    assert ospath.join(data_dir, "d3/d3.in") in fpaths
    assert ospath.join(data_dir, "d3/d3.txt") in fpaths


def test_file_paths_extn_case():
    fpaths: List[str] = []

    for fpath in FSNode.file_paths_from_dir(
        base_dir=data_dir, extensions=[".OUT"], ignore_extn_case=False
    ):
        fpaths.append(fpath)

    assert len(fpaths) == 0

    fpaths.clear()

    for fpath in FSNode.file_paths_from_dir(
        base_dir=data_dir, extensions=[".OUT"], ignore_extn_case=True
    ):
        fpaths.append(fpath)

    assert len(fpaths) == 1
    assert fpaths[0] == ospath.join(data_dir, "d3/d3.out")


def test_dir_paths():
    dpaths: List[str] = []

    for dpath in FSNode.dir_paths_from_dir(base_dir=data_dir):
        dpaths.append(dpath)

    assert len(dpaths) == 5
    assert ospath.join(data_dir, "d1") in dpaths
    assert ospath.join(data_dir, "d2") in dpaths
    assert ospath.join(data_dir, "d3") in dpaths
    assert ospath.join(data_dir, "d1/sd1") in dpaths
    assert ospath.join(data_dir, "d1/sd2") in dpaths


def test_dir_paths_with_pattern():
    dpaths: List[str] = []

    for dpath in FSNode.dir_paths_from_dir(base_dir=data_dir, dir_pattern=r"d1/sd*"):
        dpaths.append(dpath)

    assert len(dpaths) == 2
    assert ospath.join(data_dir, "d1/sd1") in dpaths
    assert ospath.join(data_dir, "d1/sd2") in dpaths

    dpaths.clear()

    for dpath in FSNode.dir_paths_from_dir(base_dir=data_dir, dir_pattern=r"d1/*"):
        dpaths.append(dpath)

    assert len(dpaths) == 3
    assert ospath.join(data_dir, "d1") in dpaths
    assert ospath.join(data_dir, "d1/sd1") in dpaths
    assert ospath.join(data_dir, "d1/sd2") in dpaths
    assert ospath.join(data_dir, "d2") not in dpaths
    assert ospath.join(data_dir, "d3") not in dpaths


def test_total_files_in_dir():
    count = FSNode.total_files_in_dir(base_dir=data_dir, extensions=[".rc"])

    assert count == 1

    count = FSNode.total_files_in_dir(base_dir=data_dir, extensions=[".in", ".txt"])

    assert count == 8

    count = FSNode.total_files_in_dir(
        base_dir=data_dir, extensions=[".IN", ".TXT"], ignore_extn_case=False
    )

    assert count == 0

    count = FSNode.total_files_in_dir(
        base_dir=data_dir, extensions=[".IN", ".TXT"], ignore_extn_case=True
    )

    assert count == 8

    count = FSNode.total_files_in_dir(base_dir="nodir", extensions=[".rc"])

    assert count == -1


def test_recursive_dir_content():
    rdirs, rfiles = FSNode.recursive_dir_content(base_dir=data_dir)

    assert len(rdirs) == 5
    assert len(rfiles) == 12

    rdirs, rfiles = FSNode.recursive_dir_content(base_dir=data_dir, extensions=[".out"])

    assert len(rdirs) == 5
    assert len(rfiles) == 1
    assert rfiles[0] == ospath.join(data_dir, "d3/d3.out")

    rdirs, rfiles = FSNode.recursive_dir_content(
        base_dir=data_dir, extensions=[".OUT"], ignore_extn_case=False
    )

    assert len(rdirs) == 5
    assert len(rfiles) == 0
