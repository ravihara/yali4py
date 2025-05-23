import hashlib
import os
import re
import socket
import sys
from typing import Iterable

import netifaces
from cachetools.func import ttl_cache
from msgspec import DecodeError, ValidationError

from .models import Failure, Result, Success


def os_uname_str():
    """Get the OS name, release, version, and machine."""
    uname_info = os.uname()
    return f"{uname_info.nodename}|{uname_info.sysname}|{uname_info.release}|{uname_info.version}|{uname_info.machine}"


def alphanum_sorted(data: Iterable):
    """Sort a list of strings in the way that humans expect."""

    def convert(text: str):
        return int(text) if text.isdigit() else text.lower()

    def alphanum_key(key):
        return [convert(c) for c in re.split("(\\d+)", key)]

    return sorted(data, key=alphanum_key)


def sizeof_object(obj, seen=None):
    """Recursively find the total size of an object including its contents."""
    size = sys.getsizeof(obj)

    if seen is None:
        seen = set()

    obj_id = id(obj)

    if obj_id in seen:
        return 0

    seen.add(obj_id)

    if isinstance(obj, dict):
        size += sum([sizeof_object(k, seen) for k in obj.keys()])
        size += sum([sizeof_object(v, seen) for v in obj.values()])
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum([sizeof_object(i, seen) for i in obj])

    return size


def get_sys_ipaddrs():
    """
    Get all IP addresses of the machine.

    Returns
    -------
    list
        List of IP addresses
    """

    ip_addresses = []

    for interface in netifaces.interfaces():
        try:
            addresses = netifaces.ifaddresses(interface)

            for addr in addresses:
                if addr == socket.AF_INET:
                    ip_addresses.append(addresses[addr][0]["addr"])
        except ValueError:
            pass

    ip_addresses.sort()

    return ip_addresses


@ttl_cache(maxsize=128, ttl=600)
def id_by_sysinfo(suffix: str = "", use_pid: bool = False, hash_algo: str = "md5"):
    """
    Generate an identifier combined with pid and ip-addresses.

    Parameters
    ----------
    suffix: str
        The suffix to be added to the identifier. Default is "".
    use_pid: bool
        True to include pid in the identifier, False otherwise. Default is False.
    hash_algo: str
        The hash algorithm to be used. Default is "md5".

    Returns
    -------
    str
        The generated identifier
    """

    ip_addresses = get_sys_ipaddrs()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    sysid = "".join(ip_addresses) + os_uname_str()

    if use_pid:
        sysid += str(os.getpid())

    if hash_algo and hash_algo in hashlib.algorithms_guaranteed:
        sysid = hashlib.new(hash_algo, sysid.encode()).hexdigest() + suffix
    else:
        sysid = hashlib.md5(sysid.encode()).hexdigest() + suffix

    return sysid


@ttl_cache(maxsize=128, ttl=600)
def filename_by_sysinfo(basename: str, extension: str = ".out"):
    """
    Get filename suffixed with hashed system info.

    Parameters
    ----------
    basename: str
        The base name of the filename
    extension: str
        The extension of the filename. Default is ".out"

    Returns
    -------
    str
        The generated filename
    """

    ip_addresses = get_sys_ipaddrs()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    suffix = "".join(ip_addresses) + os_uname_str()
    suffix = hashlib.md5(suffix.encode()).hexdigest()

    return f"{basename}_{suffix}{extension}"


def dict_to_result(data: dict) -> Result:
    """
    Convert a dictionary to a Result instance. This method will raise
    ValidationError or DecodeError if, the data does not match Success
    or Failure model

    Parameters
    ----------
    data : dict
        The dictionary to be converted

    Returns
    -------
    Result
        The Result instance
    """
    try:
        res = Success(**data)
        return res
    except TypeError:
        try:
            res = Failure(**data)
            return res
        except TypeError:
            res = Failure(error="Invalid data", extra={"data": data})
            return res
    except (ValidationError, DecodeError):
        res = Failure(error="Invalid data", extra={"data": data})
        return res
