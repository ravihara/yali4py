from typing import Any, Dict, Tuple

from yali.core.metatypes import PoolExecutorInitFunc

from .micro import YaliMicro


class YaliWeb(YaliMicro):
    def __init__(
        self,
        *,
        service_name: str,
        log_config: Dict[str, Any] | None = None,
        thread_init_func: PoolExecutorInitFunc | None = None,
        thread_init_args: Tuple = (),
        process_init_func: PoolExecutorInitFunc | None = None,
        process_init_args: Tuple = (),
    ):
        super().__init__(
            service_name=service_name,
            log_config=log_config,
            thread_init_func=thread_init_func,
            thread_init_args=thread_init_args,
            process_init_func=process_init_func,
            process_init_args=process_init_args,
        )
