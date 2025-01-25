from logging import getLogger
from logging.config import dictConfig as dict_logging_config
from typing import Any, Callable, Dict

from ..common import yali_mproc_context
from ..consts import DEFAULT_LOG_FORMAT
from ..models import BaseModel
from ..settings import log_settings
from ..strings import lower_with_hyphens
from ..typebase import NonEmptyStr, SingletonMeta
from .config import default_log_config, mproc_qlog_config
from .formatters import effective_log_level

_log_settings = log_settings()
_log_level = effective_log_level()


class LogOptions(BaseModel):
    name: NonEmptyStr = "yali_app"
    format: str = DEFAULT_LOG_FORMAT
    config: Dict[str, Any] | None = None
    post_hook: Callable[[], None] | None = None


class YaliLog(metaclass=SingletonMeta):
    def __init__(self, *, options: LogOptions):
        self._log_name = lower_with_hyphens(options.name)
        log_config = options.config or default_log_config(log_name=self._log_name)

        if _log_settings.enable_mproc_logging:
            self.__mproc_context = yali_mproc_context()
            self.__mproc_manager = self.__mproc_context.Manager()
            self.__mproc_queue = self.__mproc_manager.Queue(
                maxsize=_log_settings.log_queue_size
            )

            qlog_config = mproc_qlog_config(
                log_queue=self.__mproc_queue,
                log_config=log_config,
                log_format=options.format,
                is_main_process=True,
            )

            dict_logging_config(config=qlog_config)
        else:
            dict_logging_config(config=log_config)

        self._root = getLogger()
        self._app = getLogger(name=self._log_name)
        self._root.propagate = False

        if options.post_hook:
            options.post_hook()

    @property
    def name(self):
        return self._log_name

    @property
    def level(self):
        return _log_level

    @property
    def root_logger(self):
        return self._root

    @property
    def app_logger(self):
        return self._app

    @property
    def mproc_enabled(self):
        return _log_settings.enable_mproc_logging

    def get_logger(self, name: str = ""):
        """Get a logger by name, or the app logger if no name is provided"""
        name = name.strip() if name else self._log_name

        if name:
            return getLogger(name=name)

        return self._app
