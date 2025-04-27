import asyncio
import os
import re
from collections import ChainMap
from multiprocessing import get_context as mproc_get_context
from typing import Dict, List

import uvloop
import yaml
from decouple import Config as EnvConfig
from decouple import RepositoryEnv, RepositoryIni
from dotenv import find_dotenv, load_dotenv

from ..models import BaseModel
from ..osfiles import FSNode
from ..typebase import MprocContext


class AppConfig(BaseModel):
    mproc_ctx: MprocContext | None = None
    data: Dict = {}


## Regex pattern to capture variables with multiple fallbacks, e.g. ${VAR:-${DEFAULT_VAR:-default_value}}
__env_var_regex = re.compile(r"\$\{([^}^{]+)\}")

## Global environment configuration
__env_config: EnvConfig | None = None

## Global application configuration
__app_config: AppConfig | None = None


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


def config_data_from_yaml(yaml_conf_file: str, *, env_file: str | None = None):
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


def env_config(env_files: List[str] = []):
    global __env_config

    if __env_config:
        return __env_config

    if not env_files:
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
        raise ValueError(
            "No environment files found. Either pass env_files or set ENV_FILES environment variable to a comma-separated list of environment files. Both ini env files are supported."
        )

    __env_config = EnvConfig(ChainMap(*conf_repos))

    return __env_config


def application_config(
    *, yaml_conf_file: str | None = None, env_file: str | None = None
):
    global __app_config

    if __app_config:
        return __app_config

    print("Initializing application configuration")
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    __app_config = AppConfig()
    mproc_ctx_env = os.getenv("MULTI_PROCESS_CONTEXT", "spawn").lower().strip()

    if mproc_ctx_env == "spawn":
        __app_config.mproc_ctx = mproc_get_context("spawn")
    else:
        __app_config.mproc_ctx = mproc_get_context("fork")

    if yaml_conf_file:
        __app_config.data = config_data_from_yaml(yaml_conf_file, env_file=env_file)

    return __app_config
