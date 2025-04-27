import base64
import gzip
import io
from sys import getsizeof
from typing import Annotated, Literal

import lz4.block as lz4b
import lz4.frame as lz4f
import zstandard as zstd
from msgspec import DecodeError, ValidationError

from ..codecs import JSONNode
from ..consts import DEFAULT_COMPRESS_LEVEL
from ..models import BaseModel
from ..typebase import Constraint, JSONValue

GzipLevel = Annotated[int, Constraint.as_number(ge=0, le=9)]
ZstdLevel = Annotated[int, Constraint.as_number(ge=0, le=22)]
Lz4Level = Annotated[int, Constraint.as_number(ge=0, le=16)]


class GzipCompression(BaseModel):
    algo: Literal["gzip"] = "gzip"
    level: GzipLevel = DEFAULT_COMPRESS_LEVEL


class ZstdCompression(BaseModel):
    algo: Literal["zstd"] = "zstd"
    level: ZstdLevel = DEFAULT_COMPRESS_LEVEL


class Lz4Compression(BaseModel):
    algo: Literal["lz4"] = "lz4"
    level: Lz4Level = DEFAULT_COMPRESS_LEVEL


ArcCompression = GzipCompression | ZstdCompression | Lz4Compression


class CompressionConfig(BaseModel):
    archive: ArcCompression
    output: Literal["raw_bytes", "b64_string"] = "raw_bytes"
    encoding: str = "utf-8"


class DecompressionConfig(BaseModel):
    archive: ArcCompression
    output: Literal["raw_bytes", "raw_string", "json"] = "raw_bytes"
    encoding: str = "utf-8"


class Archiver:
    @staticmethod
    def _lz4b_decompress(data: bytes):
        """
        Decompress data using lz4b

        Parameters
        ----------
        data : bytes
            Data to decompress

        Returns
        -------
        bytes
            Decompressed data
        """
        res_data = bytes()
        comp_size = getsizeof(data)
        max_decomp_size = comp_size * 100
        usize = max_decomp_size // 16

        while True:
            try:
                res_data = lz4b.decompress(
                    data, uncompressed_size=usize, return_bytearray=False
                )
                break
            except lz4b.LZ4BlockError:
                usize *= 2

                if usize > max_decomp_size:
                    raise

        return res_data

    @staticmethod
    def compress_bytes(*, data: bytes, config: CompressionConfig):
        """
        Compress data using gzip, lz4 or zstd based on the configuration

        Parameters
        ----------
        data : bytes
            Data to compress
        config : CompressionConfig
            Compression configuration

        Returns
        -------
        bytes or base64 encoded string
            Compressed data
        """
        res_data = bytes()

        ## Default is zstd compression
        if config.archive.algo == "gzip":
            res_data = gzip.compress(data, compresslevel=config.archive.level)
        elif config.archive.algo == "lz4":
            res_data = lz4f.compress(
                data,
                compression_level=config.archive.level,
                return_bytearray=False,
                store_size=True,
            )
        else:
            zstd_compressor = zstd.ZstdCompressor(level=config.archive.level)

            with io.BytesIO() as out_bio:
                with zstd_compressor.stream_writer(out_bio) as compressor:
                    compressor.write(data)
                    compressor.flush(zstd.FLUSH_FRAME)

                    res_data = out_bio.getvalue()

            del zstd_compressor

        if config.output == "b64_string":
            res_data = base64.b64encode(res_data).decode(encoding=config.encoding)

        return res_data

    @staticmethod
    def decompress_bytes(*, data: bytes, config: DecompressionConfig):
        """
        Decompress data using gzip, lz4 or zstd based on the configuration

        Parameters
        ----------
        data : bytes
            Data to decompress
        config : DecompressionConfig
            Decompression configuration

        Returns
        -------
        bytes or string or json object
            Decompressed data
        """
        res_data = bytes()

        if config.archive.algo == "gzip":
            res_data = gzip.decompress(data)
        elif config.archive.algo == "lz4":
            try:
                res_data = lz4f.decompress(data, return_bytearray=False)
            except Exception:
                try:
                    res_data = lz4b.decompress(data, return_bytearray=False)
                except Exception:
                    res_data = Archiver._lz4b_decompress(data=data)
        else:
            zstd_decompressor = zstd.ZstdDecompressor()

            with zstd_decompressor.stream_reader(data) as stream_reader:
                res_data = stream_reader.readall()

            del zstd_decompressor

        if config.output == "raw_string":
            res_data = bytes.decode(res_data, encoding=config.encoding)
        elif config.output == "json":
            try:
                res_data = JSONNode.load_data(data=res_data)
            except (ValidationError, DecodeError):
                pass

        return res_data

    @staticmethod
    def compress_string(*, data: str, config: CompressionConfig):
        """
        Compress string using gzip, lz4 or zstd based on the configuration

        Parameters
        ----------
        data : str
            Data to compress
        config : CompressionConfig
            Compression configuration

        Returns
        -------
        bytes or base64 encoded string
            Compressed data
        """
        in_data = bytes(data, encoding=config.encoding)
        return Archiver.compress_bytes(data=in_data, config=config)

    @staticmethod
    def decompress_string(*, data: str, config: DecompressionConfig):
        """
        Decompress string using gzip, lz4 or zstd based on the configuration

        Parameters
        ----------
        data : str
            Data to decompress
        config : DecompressionConfig
            Decompression configuration

        Returns
        -------
        bytes or string or json object
            Decompressed data
        """
        try:
            in_data = base64.b64decode(data)
        except Exception:
            in_data = bytes(data, encoding=config.encoding)

        return Archiver.decompress_bytes(data=in_data, config=config)

    @staticmethod
    def compress_json(*, data: JSONValue, config: CompressionConfig):
        """
        Compress json using gzip, lz4 or zstd based on the configuration

        Parameters
        ----------
        data : JSONValue
            Data to compress
        config : CompressionConfig
            Compression configuration

        Returns
        -------
        bytes or base64 encoded string
            Compressed data
        """
        in_data = JSONNode.dump_bytes(data=data)
        return Archiver.compress_bytes(data=in_data, config=config)
