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
def sorted_alphanumeric(data: Iterable):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split(r"(\d+)", key)]
    return sorted(data, key=alphanum_key)


@staticmethod
def size_of_object(obj, seen=None):
    """Recursively find the total size of an object including its contents."""
    size = sys.getsizeof(obj)

    if seen is None:
        seen = set()

    obj_id = id(obj)

    if obj_id in seen:
        return 0

    seen.add(obj_id)

    if isinstance(obj, dict):
        size += sum([size_of_object(k, seen) for k in obj.keys()])
        size += sum([size_of_object(v, seen) for v in obj.values()])
    elif isinstance(obj, (list, tuple, set, frozenset)):
        size += sum([size_of_object(i, seen) for i in obj])

    return size


@staticmethod
def get_ip_addresses():
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
def runtime_net_uuid(hash_algo: str = "md5"):
    """Generate a unique ID combined with pid and ip-addresses.

    Args:
        hash_algo: The hashing algorithm to use. Default is "md5".

    Returns:
        A unique ID.
    """

    ip_addresses = get_ip_addresses()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    str_val = "".join(ip_addresses) + str(os.getpid()) + uuid4().hex

    if hash_algo and hash_algo in hashlib.algorithms_guaranteed:
        return hashlib.new(hash_algo, str_val.encode()).hexdigest()

    return hashlib.sha256(str_val.encode()).hexdigest()


@staticmethod
@ttl_cache(maxsize=128, ttl=600)
def runtime_net_id(suffix: str, hash_algo: str = "md5"):
    """Generate an identifier combined with pid and ip-addresses.

    Args:
        suffix: The suffix to add to the ID.
        hash_algo: The hashing algorithm to use. Default is "md5".

    Returns:
        A unique ID.
    """

    ip_addresses = get_ip_addresses()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    str_val = "".join(ip_addresses) + str(os.getpid()) + suffix

    if hash_algo and hash_algo in hashlib.algorithms_guaranteed:
        return hashlib.new(hash_algo, str_val.encode()).hexdigest()

    return hashlib.sha256(str_val.encode()).hexdigest()


@staticmethod
@ttl_cache(maxsize=128, ttl=600)
def netinfo_suffixed_name(basename: str, extension: str = ".out"):
    """Get the base name of the file.

    Args:
        basename: The name of the file.
        extension: The extension of the file. Default is ".out".

    Returns:
        The base name of the file.
    """

    ip_addresses = get_ip_addresses()

    if not ip_addresses:
        ip_addresses = ["127.0.0.1"]

    ip_addresses.sort()

    suffix = "".join(ip_addresses)
    suffix = hashlib.md5(suffix.encode()).hexdigest()

    return f"{basename}_{suffix}{extension}"
