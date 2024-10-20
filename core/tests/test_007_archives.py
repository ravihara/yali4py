import os

from core.yali.core.utils.archives import (
    Archiver,
    CompressionConfig,
    DecompressionConfig,
    GzipCompression,
    Lz4Compression,
    ZstdCompression,
)
from core.yali.core.utils.osfiles import FilesConv

test_root = os.path.dirname(os.path.abspath(__file__))

if not FilesConv.is_dir_readable(dir_path=test_root):
    test_root = "/yali-core/tests"

gzip_conf = GzipCompression()
lz4_conf = Lz4Compression()
zstd_conf = ZstdCompression()


test_bytes = (
    b"123inthisworldofpracticalrealitiesapurelybookisheducationhasnoperfectjustification!#$"
)
test_string = "Hello world from Yali!!"
test_json: dict = {"a": 1, "b": [2, 3]}


##### Test gzip (de)compression #####


def test_gzip_bytes_archiving():
    comp_config = CompressionConfig(archive=gzip_conf)
    decomp_config = DecompressionConfig(archive=gzip_conf)

    comp_data = Archiver.compress_bytes(data=test_bytes, config=comp_config)
    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)

    assert test_bytes == decomp_data


def test_gzip_string_archiving():
    comp_config = CompressionConfig(archive=gzip_conf, output="b64_string")
    decomp_config = DecompressionConfig(archive=gzip_conf, output="raw_string")

    comp_data = Archiver.compress_string(data=test_string, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, str)

    assert test_string == decomp_data


def test_gzip_json_archiving():
    comp_config = CompressionConfig(archive=gzip_conf)
    decomp_config = DecompressionConfig(archive=gzip_conf, output="json")

    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, bytes)

    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data

    comp_config.output = "b64_string"
    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data


##### Test lz4 (de)compression #####


def test_lz4_bytes_archiving():
    comp_config = CompressionConfig(archive=lz4_conf)
    decomp_config = DecompressionConfig(archive=lz4_conf)

    comp_data = Archiver.compress_bytes(data=test_bytes, config=comp_config)
    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)

    assert test_bytes == decomp_data


def test_lz4_string_archiving():
    comp_config = CompressionConfig(archive=lz4_conf, output="b64_string")
    decomp_config = DecompressionConfig(archive=lz4_conf, output="raw_string")

    comp_data = Archiver.compress_string(data=test_string, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, str)

    assert test_string == decomp_data


def test_lz4_json_archiving():
    comp_config = CompressionConfig(archive=lz4_conf)
    decomp_config = DecompressionConfig(archive=lz4_conf, output="json")

    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, bytes)

    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data

    comp_config.output = "b64_string"
    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data


##### Test zstd (de)compression #####


def test_zstd_bytes_archiving():
    comp_config = CompressionConfig(archive=zstd_conf)
    decomp_config = DecompressionConfig(archive=zstd_conf)

    comp_data = Archiver.compress_bytes(data=test_bytes, config=comp_config)
    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)

    assert test_bytes == decomp_data


def test_zstd_string_archiving():
    comp_config = CompressionConfig(archive=zstd_conf, output="b64_string")
    decomp_config = DecompressionConfig(archive=zstd_conf, output="raw_string")

    comp_data = Archiver.compress_string(data=test_string, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, str)

    assert test_string == decomp_data


def test_zstd_json_archiving():
    comp_config = CompressionConfig(archive=zstd_conf)
    decomp_config = DecompressionConfig(archive=zstd_conf, output="json")

    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, bytes)

    decomp_data = Archiver.decompress_bytes(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data

    comp_config.output = "b64_string"
    comp_data = Archiver.compress_json(data=test_json, config=comp_config)
    assert isinstance(comp_data, str)

    decomp_data = Archiver.decompress_string(data=comp_data, config=decomp_config)
    assert isinstance(decomp_data, dict)

    assert test_json == decomp_data
