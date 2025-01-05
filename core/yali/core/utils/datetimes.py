import datetime as dt
from typing import List

from ..typings import BaseModel

ALLOWED_DATETIME_FORMATS: List[str] = [
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S.%f%Z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
]


class TimeParts(BaseModel):
    days: int = 0
    hours: int = 0
    mins: int = 0
    secs: int = 0


class DateTimeConv:
    mod = dt

    @staticmethod
    def get_current_utc_time(strip_tzinfo=False) -> dt.datetime:
        """
        Get current UTC time with optionally stripped timezone info.
        Timezone info will be included by default.

        Parameters
        ----------
        strip_tzinfo: bool, optional
            True to strip timezone info, False to keep timezone info

        Returns
        -------
        datetime
            current UTC datetime object
        """
        current_time = dt.datetime.now(dt.UTC)

        if strip_tzinfo:
            current_time = current_time.replace(tzinfo=None)

        return current_time

    @staticmethod
    def get_current_local_time(strip_tzinfo=False) -> dt.datetime:
        """
        Get current local time with optionally stripped timezone info.
        Timezone info will be included by default.

        Parameters
        ----------
        strip_tzinfo: bool, optional
            True to strip timezone info, False to keep timezone info

        Returns
        -------
        datetime
            current local datetime object
        """
        current_time = dt.datetime.now()

        if strip_tzinfo:
            current_time = current_time.replace(tzinfo=None)

        return current_time

    @staticmethod
    def str_to_datetime(dt_str: str):
        """
        Create datetime object from supported date-time formatted string

        Parameters
        ----------
        dt_str: str
            The date-time formatted string to be converted into datetime object

        Returns
        -------
        datetime
            datetime object representing the given string
        """
        for fmt in ALLOWED_DATETIME_FORMATS:
            try:
                return dt.datetime.strptime(dt_str, fmt)
            except ValueError:
                pass

        raise ValueError(f"Unsupported datetime string: {dt_str}")

    @staticmethod
    def exponential_backoff(base_delay: int, max_retries: int, max_delay: int):
        """
        Get a generator function for exponential backoff

        Parameters
        ----------
        base_delay: int
            Initial backoff delay in seconds
        max_retries: int
            Maximum number of retries allowed for backoff
        max_delay: int
            Maximum delay in seconds allowed for backoff

        Returns
        -------
        Generator
            generator function that yields backoff delay in seconds
        """
        retries: int = 0

        while retries < max_retries:
            delay: int = base_delay * (2**retries)
            yield min(delay, max_delay)

            retries += 1

    @staticmethod
    def time_as_parts(seconds: int):
        """
        Splits the time in seconds to days, hours, minutes, seconds and returns

        Parameters
        ----------
        seconds: int
            Seconds value, typically a large number that needs to be split into time-parts

        Returns
        -------
        TimeParts
            The TimeParts object containing days, hours, mins and secs attributes

        """
        min_in_secs = 60
        hr_in_secs = 3600

        secs = seconds
        hrs = secs // hr_in_secs
        secs = secs % hr_in_secs
        mins = secs // min_in_secs
        secs = secs % min_in_secs

        if hrs >= 24:
            days = hrs // 24
            hrs = hrs % 24

            return TimeParts(days=days, hours=hrs, mins=mins, secs=secs)

        return TimeParts(hours=hrs, mins=mins, secs=secs)

    @staticmethod
    def time_diff_seconds(start_dt: dt.datetime, end_dt: dt.datetime):
        """
        Finds the difference between start and end date-time values, in seconds and returns it.

        Parameters
        ----------
        start_dt: datetime
            Start datetime value
        end_dt: datetime
            End datetime value

        Returns
        -------
        float
            The difference between end-time and start-time, in seconds
        """
        sdt_utc = start_dt.replace(tzinfo=dt.UTC)
        edt_utc = end_dt.replace(tzinfo=dt.UTC)

        diff = (edt_utc - sdt_utc).total_seconds()

        return diff
