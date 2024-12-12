import json
import os
import re
import tomllib
from re import Pattern as RegExPattern
from typing import Dict, List

import yaml

from ..typings import YaliError


def _select_matching_file(
    fentry: os.DirEntry[str], extensions: List[str], ignore_extn_case: bool = True
):
    if not extensions:
        return fentry.path

    if ignore_extn_case:
        if os.path.splitext(fentry.name)[1].lower() in extensions:
            return fentry.path

        return None

    if os.path.splitext(fentry.name)[1] in extensions:
        return fentry.path

    return None


def _file_paths_from_dir(
    *,
    base_dir: str,
    extensions: List[str],
    follow_symlinks: bool,
    ignore_extn_case: bool,
    file_pattern: RegExPattern | None = None,
):
    for entry in os.scandir(base_dir):
        if entry.is_dir(follow_symlinks=follow_symlinks):
            yield from _file_paths_from_dir(
                base_dir=entry.path,
                extensions=extensions,
                follow_symlinks=follow_symlinks,
                ignore_extn_case=ignore_extn_case,
                file_pattern=file_pattern,
            )
        else:
            fpath = _select_matching_file(
                fentry=entry, extensions=extensions, ignore_extn_case=ignore_extn_case
            )

            if not fpath:
                continue

            if file_pattern and file_pattern.search(fpath) is None:
                continue

            yield fpath


def _dir_paths_from_dir(
    *,
    base_dir: str,
    follow_symlinks: bool,
    ignore_extn_case: bool,
    dir_pattern: RegExPattern | None = None,
):
    for entry in os.scandir(base_dir):
        if not entry.is_dir(follow_symlinks=follow_symlinks):
            continue

        if not dir_pattern or dir_pattern.search(entry.path):
            yield entry.path

        yield from _dir_paths_from_dir(
            base_dir=entry.path,
            follow_symlinks=follow_symlinks,
            ignore_extn_case=ignore_extn_case,
            dir_pattern=dir_pattern,
        )


def _total_files_in_dir(
    *, base_dir: str, extensions: List[str], follow_symlinks: bool, ignore_extn_case: bool
):
    count: int = 0

    for entry in os.scandir(base_dir):
        if entry.is_dir(follow_symlinks=follow_symlinks):
            count += _total_files_in_dir(
                base_dir=entry.path,
                extensions=extensions,
                follow_symlinks=follow_symlinks,
                ignore_extn_case=ignore_extn_case,
            )
        elif _select_matching_file(
            fentry=entry, extensions=extensions, ignore_extn_case=ignore_extn_case
        ):
            count += 1

    return count


def _recursive_dir_content(
    *, base_dir: str, extensions: List[str], ignore_extn_case: bool, follow_symlinks: bool
):
    res_dirs: List[str] = []
    res_files: List[str] = []

    for f in os.scandir(base_dir):
        if f.is_dir(follow_symlinks=follow_symlinks):
            res_dirs.append(f.path)
        elif f.is_file():
            fpath = _select_matching_file(
                fentry=f, extensions=extensions, ignore_extn_case=ignore_extn_case
            )

            if fpath:
                res_files.append(fpath)

    for rdir in list(res_dirs):
        flds, fls = _recursive_dir_content(
            base_dir=rdir,
            extensions=extensions,
            ignore_extn_case=ignore_extn_case,
            follow_symlinks=follow_symlinks,
        )
        res_dirs.extend(flds)
        res_files.extend(fls)

    return res_dirs, res_files


class FilesConv:
    @staticmethod
    def file_exists(file_path: str) -> bool:
        """Check if the given file path refers to an existing file"""
        return os.path.exists(file_path) and os.path.isfile(file_path)

    @staticmethod
    def dir_exists(dir_path: str) -> bool:
        """Check if the given directory path refers to an existing directory"""
        return os.path.exists(dir_path) and os.path.isdir(dir_path)

    @staticmethod
    def is_file_readable(file_path: str) -> bool:
        """Check if the given file path refers to a readable file"""
        if not file_path:
            return False

        if os.path.exists(file_path):
            return os.path.isfile(file_path) and os.access(file_path, os.R_OK)

        return False

    @staticmethod
    def is_file_writable(file_path: str, check_creatable: bool = False) -> bool:
        """
        Check if the given file path refers to a writable file.

        Parameters
        ----------
        file_path: str
            The file path to be checked
        check_creatable: bool
            True to check if the file can be created, False otherwise

        Returns
        -------
        bool
            True if the file is writable, False otherwise
        """
        if not file_path:
            return False

        if os.path.exists(file_path):
            return os.path.isfile(file_path) and os.access(file_path, os.W_OK)

        if not check_creatable:
            return False

        if file_path[0] == "~":
            abs_path = os.path.expanduser(file_path)
        else:
            abs_path = os.path.abspath(file_path)

        pdir = os.path.dirname(abs_path)

        if not os.path.exists(pdir):
            return False

        return os.access(pdir, os.W_OK)

    @staticmethod
    def is_dir_readable(dir_path: str) -> bool:
        """Check if the given directory path refers to a readable folder"""
        if not dir_path:
            return False

        if os.path.exists(dir_path):
            return os.path.isdir(dir_path) and os.access(dir_path, os.R_OK)

        return False

    @staticmethod
    def is_dir_writable(dir_path: str, check_creatable: bool = False) -> bool:
        """
        Check if the given directory path refers to a writable folder.

        Parameters
        ----------
        dir_path: str
            The directory path to be checked
        check_creatable: bool
            True to check if the directory can be created, False otherwise

        Returns
        -------
        bool
            True if the directory is writable, False otherwise
        """
        if not dir_path:
            return False

        if os.path.exists(dir_path):
            return os.path.isdir(dir_path) and os.access(dir_path, os.W_OK)

        if not check_creatable:
            return False

        if dir_path[0] == "~":
            abs_path = os.path.expanduser(dir_path)
        else:
            abs_path = os.path.abspath(dir_path)

        pdir = os.path.dirname(abs_path)

        if not os.path.exists(pdir):
            return False

        return os.access(pdir, os.W_OK)

    @staticmethod
    def recursive_dir_content(
        *,
        base_dir: str,
        extensions: List[str] = [],
        ignore_extn_case: bool = True,
        follow_symlinks: bool = True,
    ):
        """
        Recursively scan a directory and collect list of file and directory paths.

        Parameters
        ----------
        base_dir: str
            The base directory to be scanned
        extensions: List[str]
            List of extensions to be matched
        ignore_extn_case: bool
            True to ignore the case of extensions, False otherwise
        follow_symlinks: bool
            True to follow symlinks, False otherwise

        Returns
        -------
        Tuple[List[str], List[str]]
            List of directory paths, List of file paths
        """
        if ignore_extn_case:
            extns = [extn.lower() for extn in extensions]
        else:
            extns = extensions

        return _recursive_dir_content(
            base_dir=base_dir,
            extensions=extns,
            ignore_extn_case=ignore_extn_case,
            follow_symlinks=follow_symlinks,
        )

    @staticmethod
    def total_files_in_dir(
        *,
        base_dir: str,
        extensions: List[str] = [],
        follow_symlinks: bool = True,
        ignore_extn_case: bool = True,
    ):
        """
        Recursively scan a directory and count its files.

        Parameters
        ----------
        base_dir: str
            The base directory to be scanned
        extensions: List[str]
            List of extensions to be matched
        follow_symlinks: bool
            True to follow symlinks, False otherwise
        ignore_extn_case: bool
            True to ignore the case of extensions, False otherwise

        Returns
        -------
        int
            The number of files
        """
        if not FilesConv.is_dir_readable(base_dir):
            return -1

        if ignore_extn_case:
            extns = [extn.lower() for extn in extensions]
        else:
            extns = extensions

        return _total_files_in_dir(
            base_dir=base_dir,
            extensions=extns,
            follow_symlinks=follow_symlinks,
            ignore_extn_case=ignore_extn_case,
        )

    @staticmethod
    def file_paths_from_dir(
        *,
        base_dir: str,
        extensions: List[str],
        file_pattern: str | None = None,
        follow_symlinks: bool = True,
        ignore_extn_case: bool = True,
    ):
        """
        Get a generator function to recursively scan a directory and collect list of file paths.

        Parameters
        ----------
        base_dir: str
            The base directory to be scanned
        extensions: List[str]
            List of extensions to be matched
        file_pattern: str | None
            Regular expression pattern to match file paths
        follow_symlinks: bool
            True to follow symlinks, False otherwise
        ignore_extn_case: bool
            True to ignore the case of extensions, False otherwise

        Returns
        -------
        Generator
            Generator function to yield file paths
        """
        if not FilesConv.is_dir_readable(base_dir):
            return ""

        if ignore_extn_case:
            extns = [extn.lower() for extn in extensions]
        else:
            extns = extensions

        if file_pattern:
            regex_pattern = re.compile(file_pattern)

        yield from _file_paths_from_dir(
            base_dir=base_dir,
            extensions=extns,
            follow_symlinks=follow_symlinks,
            ignore_extn_case=ignore_extn_case,
            file_pattern=regex_pattern if file_pattern else None,
        )

    @staticmethod
    def dir_paths_from_dir(
        *,
        base_dir: str,
        dir_pattern: str | None = None,
        follow_symlinks: bool = True,
        ignore_extn_case: bool = True,
    ):
        """
        Generator function to recursively scan a directory and collect list of directory paths.

        Parameters
        ----------
        base_dir: str
            The base directory to be scanned
        dir_pattern: str | None
            Regular expression pattern to match directory paths
        follow_symlinks: bool
            True to follow symlinks, False otherwise
        ignore_extn_case: bool
            True to ignore the case of extensions, False otherwise

        Returns
        -------
        Generator
            Generator function to yield directory paths
        """
        if not FilesConv.is_dir_readable(base_dir):
            return ""

        if dir_pattern:
            regex_pattern = re.compile(dir_pattern)

        yield from _dir_paths_from_dir(
            base_dir=base_dir,
            follow_symlinks=follow_symlinks,
            ignore_extn_case=ignore_extn_case,
            dir_pattern=regex_pattern if dir_pattern else None,
        )

    @staticmethod
    def read_bytes(file_path: str):
        """
        Read bytes from a file

        Parameters
        ----------
        file_path: str
            The file path

        Returns
        -------
        bytes
            The bytes read from the file
        """
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"File '{file_path}' is not readable")

        with open(file_path, "rb") as f:
            return f.read()

    @staticmethod
    def read_text(file_path: str):
        """
        Read text from a file

        Parameters
        ----------
        file_path: str
            The file path

        Returns
        -------
        str
            The text read from the file
        """
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"Text file '{file_path}' is not readable")

        with open(file_path, "r") as f:
            return f.read()

    @staticmethod
    def read_json(file_path: str, decorder_cls: json.JSONDecoder | None = None) -> Dict:
        """
        Read json from a file

        Parameters
        ----------
        file_path: str
            The file path
        decorder_cls: json.JSONDecoder
            The json decoder class

        Returns
        -------
        Dict
            The json read from the file
        """
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"Json file '{file_path}' is not readable")

        with open(file_path, "r") as f:
            return json.load(f, cls=decorder_cls)

    @staticmethod
    def read_yaml(file_path: str) -> Dict:
        """
        Read yaml from a file

        Parameters
        ----------
        file_path: str
            The file path

        Returns
        -------
        Dict
            The yaml read from the file
        """
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"Yaml file '{file_path}' is not readable")

        with open(file_path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def read_toml(file_path: str) -> Dict:
        """
        Read toml from a file

        Parameters
        ----------
        file_path: str
            The file path

        Returns
        -------
        Dict
            The toml read from the file
        """
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"Toml file '{file_path}' is not readable")

        with open(file_path, "rb") as f:
            return tomllib.load(f)

    @staticmethod
    def write_bytes(file_path: str, data: bytes, *, overwrite: bool = False):
        """
        Write bytes to a file

        Parameters
        ----------
        file_path: str
            The file path
        data: bytes
            The bytes to be written
        overwrite: bool
            True to overwrite the file, False otherwise

        Returns
        -------
        int
            The number of bytes written
        """
        if not FilesConv.is_file_writable(file_path, check_creatable=True):
            raise YaliError(f"File '{file_path}' is not writable")

        if not overwrite and FilesConv.file_exists(file_path):
            return -1

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "wb") as f:
            return f.write(data)

    @staticmethod
    def write_text(file_path: str, data: str, *, overwrite: bool = False):
        """
        Write text to a file

        Parameters
        ----------
        file_path: str
            The file path
        data: str
            The text to be written
        overwrite: bool
            True to overwrite the file, False otherwise

        Returns
        -------
        int
            The number of bytes written
        """
        if not FilesConv.is_file_writable(file_path, check_creatable=True):
            raise YaliError(f"Text file '{file_path}' is not writable")

        if not overwrite and FilesConv.file_exists(file_path):
            return -1

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as f:
            return f.write(data)

    @staticmethod
    def write_json(
        file_path: str,
        data: Dict,
        *,
        overwrite: bool = False,
        encoder_cls: json.JSONEncoder | None = None,
    ):
        """
        Write json to a file

        Parameters
        ----------
        file_path: str
            The file path
        data: Dict
            The json to be written
        overwrite: bool
            True to overwrite the file, False otherwise
        encoder_cls: json.JSONEncoder
            The json encoder class

        Returns
        -------
        None
        """
        if not FilesConv.is_file_writable(file_path, check_creatable=True):
            raise YaliError(f"Json file '{file_path}' is not writable")

        if not overwrite and FilesConv.file_exists(file_path):
            return

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as f:
            json.dump(data, f, cls=encoder_cls)

    @staticmethod
    def delete_file(file_path: str):
        """
        Delete a file

        Parameters
        ----------
        file_path: str
            The file path

        Returns
        -------
        None
        """
        if not FilesConv.is_file_writable(file_path):
            raise YaliError(f"File '{file_path}' is not writable")

        os.remove(file_path)

    @staticmethod
    def delete_dir(dir_path: str):
        """
        Delete a directory

        Parameters
        ----------
        dir_path: str
            The directory path

        Returns
        -------
        None
        """
        if not FilesConv.is_dir_writable(dir_path):
            raise YaliError(f"Directory '{dir_path}' is not writable")

        os.rmdir(dir_path)
