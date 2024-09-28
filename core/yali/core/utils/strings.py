import re
import string
from abc import ABC, abstractmethod
from io import StringIO
from typing import List

from ..typings import FlexiTypesModel, YaliError

DEFAULT_DELIMITERS = " -_"
ALLCHARS_REGEX = r"[{}]+"


class TokenMarkerArgs(FlexiTypesModel):
    in_buffer: StringIO
    out_buffer: StringIO
    join_char: str = ""
    delimiters: str = DEFAULT_DELIMITERS


class TokenMarker(ABC):
    def __init__(self, tmargs: TokenMarkerArgs):
        self._delimiters = tmargs.delimiters
        self._join_char = tmargs.join_char
        self._in_buffer = tmargs.in_buffer
        self._out_buffer = tmargs.out_buffer

    @abstractmethod
    def mark(self, curr_char: str, prev_char: str | None) -> bool:
        raise NotImplementedError()


class OnDelimiterNextUpperMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if curr_char not in self._delimiters:
            return False

        self._out_buffer.write(self._join_char)
        self._out_buffer.write(self._in_buffer.read(1).upper())

        return True


class OnDelimiterNextLowerMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if curr_char not in self._delimiters:
            return False

        self._out_buffer.write(self._join_char)
        self._out_buffer.write(self._in_buffer.read(1).lower())

        return True


class OnLowerUpperAppendUpperMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if prev_char == None or not prev_char.isalpha() or not prev_char.islower() or not curr_char.isupper():
            return False

        self._out_buffer.write(self._join_char)
        self._out_buffer.write(curr_char)

        return True


class OnLowerUpperAppendLowerMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if prev_char == None or not prev_char.isalpha() or not prev_char.islower() or not curr_char.isupper():
            return False

        self._out_buffer.write(self._join_char)
        self._out_buffer.write(curr_char.lower())

        return True


class OnUpperUpperAppendJoinMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if prev_char == None or not prev_char.isalpha() or not prev_char.isupper() or not curr_char.isupper():
            return False

        self._out_buffer.write(self._join_char)
        self._out_buffer.write(curr_char)

        return True


class OnUpperUpperAppendCurrentMarker(TokenMarker):
    def __init__(self, tmargs: TokenMarkerArgs):
        super().__init__(tmargs)

    def mark(self, curr_char: str, prev_char: str | None = None):
        if prev_char == None or not prev_char.isalpha() or not prev_char.isupper() or not curr_char.isupper():
            return False

        self._out_buffer.write(curr_char)

        return True


class StringConv:
    @staticmethod
    def _prepared_string(in_str: str, delimiters: str, clear_punctuation: bool) -> str:
        if not in_str:
            raise YaliError("Empty string passed for conversion")

        ## Step 1: Strip delimiters
        out_str = in_str.strip(delimiters)

        ## Step 2: Remove non-delimiter punctuation
        if clear_punctuation:
            punc = "".join([ch for ch in string.punctuation if ch not in delimiters])
            out_str = re.sub(ALLCHARS_REGEX.format(re.escape(punc)), "", out_str)

        ## Step 3: Replace recurring delimiters with a single one
        out_str = re.sub(ALLCHARS_REGEX.format(re.escape(delimiters)), delimiters[0], out_str)

        ## Step 4: Convert the string to lowercase
        out_str = out_str.lower() if out_str.isupper() else out_str

        return out_str

    @staticmethod
    def _process_markers(
        token_markers: List[TokenMarker],
        in_buffer: StringIO,
        out_buffer: StringIO,
        unmarked_upper: bool = False,
    ):
        prev_ch = None
        curr_ch = in_buffer.read(1)

        while curr_ch:
            is_marked = False

            for marker in token_markers:
                if marker.mark(curr_char=curr_ch, prev_char=prev_ch):
                    is_marked = True
                    break

            if not is_marked:
                out_buffer.write(curr_ch.upper() if unmarked_upper else curr_ch.lower())

            prev_ch = curr_ch
            curr_ch = in_buffer.read(1)

    @staticmethod
    def to_kebabcase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            delimiters=delimiters,
            join_char="-",
        )
        token_markers: List[TokenMarker] = [
            OnDelimiterNextLowerMarker(tmargs),
            OnLowerUpperAppendLowerMarker(tmargs),
        ]

        StringConv._process_markers(token_markers=token_markers, in_buffer=in_buffer, out_buffer=out_buffer)

        return out_buffer.getvalue()

    @staticmethod
    def to_camelcase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(in_buffer=in_buffer, out_buffer=out_buffer, delimiters=delimiters)
        token_markers: List[TokenMarker] = [
            OnDelimiterNextUpperMarker(tmargs),
            OnLowerUpperAppendUpperMarker(tmargs),
        ]

        StringConv._process_markers(token_markers=token_markers, in_buffer=in_buffer, out_buffer=out_buffer)

        return out_buffer.getvalue()

    @staticmethod
    def to_pascalcase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(in_buffer=in_buffer, out_buffer=out_buffer, delimiters=delimiters)
        token_markers: List[TokenMarker] = [
            OnDelimiterNextUpperMarker(tmargs),
            OnLowerUpperAppendUpperMarker(tmargs),
            OnUpperUpperAppendCurrentMarker(tmargs),
        ]

        out_buffer.write(in_buffer.read(1).upper())
        StringConv._process_markers(token_markers=token_markers, in_buffer=in_buffer, out_buffer=out_buffer)

        return out_buffer.getvalue()

    @staticmethod
    def to_snakecase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            delimiters=delimiters,
            join_char="_",
        )
        token_markers: List[TokenMarker] = [
            OnDelimiterNextLowerMarker(tmargs),
            OnLowerUpperAppendLowerMarker(tmargs),
        ]

        StringConv._process_markers(token_markers=token_markers, in_buffer=in_buffer, out_buffer=out_buffer)

        return out_buffer.getvalue()

    @staticmethod
    def to_cobolcase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        join_ch = "-"

        if in_str.isupper():
            return re.sub(ALLCHARS_REGEX.format(re.escape(delimiters)), join_ch, in_str)

        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            delimiters=delimiters,
            join_char=join_ch,
        )
        token_markers: List[TokenMarker] = [
            OnDelimiterNextUpperMarker(tmargs),
            OnLowerUpperAppendUpperMarker(tmargs),
        ]

        StringConv._process_markers(
            token_markers=token_markers,
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            unmarked_upper=True,
        )

        return out_buffer.getvalue()

    @staticmethod
    def to_macrocase(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        join_ch = "_"

        if in_str.isupper():
            return re.sub(ALLCHARS_REGEX.format(re.escape(delimiters)), join_ch, in_str)

        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            delimiters=delimiters,
            join_char=join_ch,
        )
        token_markers: List[TokenMarker] = [
            OnDelimiterNextUpperMarker(tmargs),
            OnLowerUpperAppendUpperMarker(tmargs),
            OnUpperUpperAppendJoinMarker(tmargs),
        ]

        StringConv._process_markers(
            token_markers=token_markers,
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            unmarked_upper=True,
        )

        return out_buffer.getvalue()

    @staticmethod
    def to_flatlower(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(in_buffer=in_buffer, out_buffer=out_buffer, delimiters=delimiters)
        token_markers: List[TokenMarker] = [
            OnDelimiterNextLowerMarker(tmargs),
            OnLowerUpperAppendLowerMarker(tmargs),
        ]

        StringConv._process_markers(token_markers=token_markers, in_buffer=in_buffer, out_buffer=out_buffer)

        return out_buffer.getvalue()

    @staticmethod
    def to_flatupper(
        in_str: str,
        delimiters: str = DEFAULT_DELIMITERS,
        clear_punctuation: bool = True,
    ) -> str:
        out_str = StringConv._prepared_string(in_str=in_str, delimiters=delimiters, clear_punctuation=clear_punctuation)

        in_buffer = StringIO(out_str)
        out_buffer = StringIO()

        tmargs = TokenMarkerArgs(in_buffer=in_buffer, out_buffer=out_buffer, delimiters=delimiters)
        token_markers: List[TokenMarker] = [
            OnDelimiterNextUpperMarker(tmargs),
            OnLowerUpperAppendUpperMarker(tmargs),
        ]

        StringConv._process_markers(
            token_markers=token_markers,
            in_buffer=in_buffer,
            out_buffer=out_buffer,
            unmarked_upper=True,
        )

        return out_buffer.getvalue()
