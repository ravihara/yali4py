from io import BytesIO

from .typebase import NonEmptyStr


class YaliError(Exception):
    def __init__(self, error: NonEmptyStr, exc_cause: BaseException | None = None):
        super().__init__(error)
        self.exc_cause = exc_cause

    def __str__(self):
        if self.exc_cause:
            exc_str = super().__str__()
            return f"{exc_str} (caused by {repr(self.exc_cause)})"

        return super().__str__()


ErrorOrBytesIO = YaliError | BytesIO
ErrorOrStr = YaliError | str
