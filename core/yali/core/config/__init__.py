import asyncio
import os
import re
from collections import ChainMap
from multiprocessing import get_context as mproc_get_context
from typing import List

import uvloop
import yaml
from decouple import AutoConfig, RepositoryEnv, RepositoryIni
from decouple import Config as EnvConfig
from dotenv import find_dotenv, load_dotenv

from ..typebase import MprocContext
from ..utils.osfiles import FSNode

## Regex pattern to capture variables with multiple fallbacks, e.g. ${VAR:-${DEFAULT_VAR:-default_value}}
__env_var_regex = re.compile(r"\$\{([^}^{]+)\}")

## Global environment configuration
__env_config: EnvConfig | None = None

## Global multi-process context
__mproc_ctx: MprocContext | None = None


def replace_env_var(match):
    expr = match.group(1)

    # Split on ':-' to handle multiple fallbacks
    fallbacks = expr.split(":-")

    # If there's only one fallback, return it
    if len(fallbacks) == 1:
        return os.getenv(fallbacks[0], None)

    # Try each fallback and return the first one that's found
    for fallback in fallbacks:
        value = os.getenv(fallback, None)
        if value is not None:
            return value

    # If no environment variable is found, return the last fallback (could be default value or empty)
    return fallbacks[-1]  # return the last fallback (could be empty)


def expand_env_vars_in_string(content):
    """
    Expands environment variables with multiple fallbacks.
    Keeps expanding until no changes are made.
    """
    global __env_var_regex

    while True:
        # Process all found variables and attempt to expand them
        new_content = __env_var_regex.sub(replace_env_var, content)

        if new_content == content:
            break

        content = new_content

    return content


def load_environment(env_file: str | None = None):
    if env_file:
        dotenv_path = find_dotenv(env_file)

        if not dotenv_path:
            default_env = find_dotenv()

            if default_env:
                load_dotenv(dotenv_path=default_env)

            return

        if not FSNode.is_file_readable(dotenv_path):
            raise ValueError(f"Environment file '{dotenv_path}' is not readable")

        load_dotenv(dotenv_path=dotenv_path)
    else:
        default_env = find_dotenv()

        if default_env:
            load_dotenv(dotenv_path=default_env)


def config_from_yaml(yaml_conf_file: str, *, env_file: str | None = None):
    """
    Loads YAML, expanding environment variables with multiple fallbacks.
    """
    load_environment(env_file=env_file)

    if not FSNode.is_file_readable(yaml_conf_file):
        raise ValueError(f"Yaml file '{yaml_conf_file}' is not readable")

    with open(yaml_conf_file, "r") as f:
        raw_content = f.read()

    # Expand environment variables
    expanded_content = expand_env_vars_in_string(raw_content)

    # Parse and load the YAML
    return yaml.safe_load(expanded_content)


def env_config():
    global __env_config

    if __env_config:
        return __env_config

    env_files = os.getenv("ENV_FILES", ".env").split(",")
    conf_repos: List[RepositoryEnv | RepositoryIni] = []

    for env_file in env_files:
        env_file = env_file.strip()

        if not FSNode.is_file_readable(env_file):
            continue

        if env_file.endswith(".env"):
            conf_repos.append(RepositoryEnv(env_file))
        elif env_file.endswith(".ini"):
            conf_repos.append(RepositoryIni(env_file))

    if not conf_repos:
        __env_config = AutoConfig()
        assert isinstance(__env_config, EnvConfig)
    else:
        __env_config = EnvConfig(ChainMap(*conf_repos))

    return __env_config


def yali_init():
    global __mproc_ctx

    if __mproc_ctx:
        return

    print("Initializing application configuration")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    mproc_ctx_env = os.getenv("MULTI_PROCESS_CONTEXT", "spawn").lower().strip()

    if mproc_ctx_env == "fork":
        __mproc_ctx = mproc_get_context("fork")
    else:
        __mproc_ctx = mproc_get_context("spawn")


## Ensure that yali_init() has been called before using yali_mproc_context
def yali_mproc_context():
    global __mproc_ctx
    assert __mproc_ctx is not None

    return __mproc_ctx
