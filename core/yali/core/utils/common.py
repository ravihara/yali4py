import json
import re
import sys
from typing import Iterable, Any
import hashlib
import os
import socket
from uuid import uuid4
import netifaces
from cachetools.func import ttl_cache


@staticmethod
def os_uname_str():
    uname_info = os.uname()
    return f"{uname_info.nodename}|{uname_info.sysname}|{uname_info.release}|{uname_info.version}|{uname_info.machine}"


@staticmethod
def is_json_data(data: Any):
    return isinstance(data, (dict, list))


@staticmethod
def safe_load_json(data: str, **kwargs):
    try:
        payload = json.loads(data, **kwargs)
        return payload
    except json.JSONDecodeError:
        return data


@staticmethod
def alphanum_sorted(data: Iterable):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split(r"(\d+)", key)]
    return sorted(data, key=alphanum_key)


@staticmethod
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


@staticmethod
def get_sys_ipaddrs():
    """Get all IP addresses of the machine.

    Returns:
        A list of IP addresses.
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

    return ip_addresses


@staticmethod
@ttl_cache(maxsize=128, ttl=600)
def id_by_proc(suffix: str, hash_algo: str = "sha256"):
    """Generate an identifier combined with pid and ip-addresses.

    Args:
        suffix: The suffix to add to the ID.
        hash_algo: The hashing algorithm to use. Default is "sha256".

    Returns:
        A unique ID.
    """

    ip_addresses = get_sys_ipaddrs()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    str_val = "".join(ip_addresses) + os_uname_str() + str(os.getpid())

    if hash_algo and hash_algo in hashlib.algorithms_guaranteed:
        id_val = hashlib.new(hash_algo, str_val.encode()).hexdigest() + suffix
    else:
        id_val = hashlib.sha256(str_val.encode()).hexdigest() + suffix

    return id_val


@staticmethod
@ttl_cache(maxsize=128, ttl=600)
def syshash_filename(basename: str, extension: str = ".out"):
    """Get filename suffixed with hashed system info.

    Args:
        basename: The name of the file.
        extension: The extension of the file. Default is ".out".

    Returns:
        The suffixed filename.
    """

    ip_addresses = get_sys_ipaddrs()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    suffix = "".join(ip_addresses) + os_uname_str()
    suffix = hashlib.md5(suffix.encode()).hexdigest()

    return f"{basename}_{suffix}{extension}"


@staticmethod
def syshash_id():
    """Get identifier based on hashed system info.

    Returns:
        The identifier.
    """

    ip_addresses = get_sys_ipaddrs()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    sysid = "".join(ip_addresses) + os_uname_str()
    sysid = hashlib.md5(sysid.encode()).hexdigest()

    return sysid
