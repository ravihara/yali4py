import os
import re
from collections import ChainMap
from typing import List

import yaml
from decouple import Config as EnvConfig
from decouple import RepositoryEnv, RepositoryIni

from ..osfiles import FSNode

## Global environment configuration
__env_config: EnvConfig | None = None

## Regex pattern to capture variables with multiple fallbacks, e.g. ${VAR:-${DEFAULT_VAR:-default_value}}
__env_var_regex = re.compile(r"\$\{([^}^{]+)\}")


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


def config_from_yaml(yaml_conf_file: str):
    """
    Loads YAML, expanding environment variables with multiple fallbacks.
    """
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
            "No environment files found. Please set ENV_FILES environment variable with comma separated list of environment files (.env or .ini) or, pass it as a list of file paths."
        )

    __env_config = EnvConfig(ChainMap(*conf_repos))

    return __env_config
