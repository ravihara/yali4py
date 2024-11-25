import os
import re
import json
import yaml
from re import Pattern as RegExPattern
from typing import List, Literal

from ..typings import YaliError

FileFormat = Literal["bytes", "text", "json", "yaml"]


# Internal function to select a file-path with matching extensions. Here
# extensions are assumed to be in lowercase if ignore_extn_case is True.
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


# Internal generator function to recursively scan a directory and collect list of file paths.
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


# Internal generator function to recursively scan a directory and collect list of directory paths.
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


# Internal function to recursively scan a directory and collect its content.
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
        return os.path.exists(file_path) and os.path.isfile(file_path)

    @staticmethod
    def dir_exists(dir_path: str) -> bool:
        return os.path.exists(dir_path) and os.path.isdir(dir_path)

    @staticmethod
    def is_file_readable(file_path: str) -> bool:
        """
        Check if the given file path refers to a readable file
        """
        if not file_path:
            return False

        if os.path.exists(file_path):
            return os.path.isfile(file_path) and os.access(file_path, os.R_OK)

        return False

    @staticmethod
    def is_file_writable(file_path: str, check_creatable: bool = False) -> bool:
        """
        Check if the given file path refers to a writable file.
        If 'check_creatable' is True (Default = False), it will check if the file
        can be created under its parent folder, if not already exists.
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
        """
        Check if the given directory path refers to a readable folder
        """
        if not dir_path:
            return False

        if os.path.exists(dir_path):
            return os.path.isdir(dir_path) and os.access(dir_path, os.R_OK)

        return False

    @staticmethod
    def is_dir_writable(dir_path: str, check_creatable: bool = False) -> bool:
        """
        Check if the given directory path refers to a writable folder.
        If 'check_creatable' is True (Default = False), it will check if the folder
        can be created under its parent folder, if not already exists.
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
    def read_file(
        file_path: str,
        read_out: FileFormat = "bytes",
        json_decoder_cls: json.decoder.JSONDecoder | None = None,
    ):
        if not FilesConv.is_file_readable(file_path):
            raise YaliError(f"File '{file_path}' is not readable")

        if read_out == "bytes":
            with open(file_path, "rb") as f:
                return f.read()

        with open(file_path, "r") as f:
            if read_out == "json":
                return json.load(f, cls=json_decoder_cls)

            if read_out == "yaml":
                return yaml.safe_load(f)

            return f.read()

    @staticmethod
    def write_file(
        file_path: str, data: bytes, *, write_out: FileFormat = "bytes", overwrite: bool = False
    ):
        if not FilesConv.is_file_writable(file_path, check_creatable=True):
            raise YaliError(f"File '{file_path}' is not writable")

        if not overwrite and FilesConv.file_exists(file_path):
            return

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if write_out == "bytes":
            with open(file_path, "wb") as f:
                return f.write(data)

        with open(file_path, "w") as f:
            if write_out == "json":
                return json.dump(data, f)

            if write_out == "yaml":
                return yaml.safe_dump(data, f)

            return f.write(data)

    @staticmethod
    def delete_file(file_path: str):
        if not FilesConv.is_file_writable(file_path):
            raise YaliError(f"File '{file_path}' is not writable")

        os.remove(file_path)

    @staticmethod
    def delete_dir(dir_path: str):
        if not FilesConv.is_dir_writable(dir_path):
            raise YaliError(f"Directory '{dir_path}' is not writable")

        os.rmdir(dir_path)
