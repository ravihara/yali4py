import logging

from litestar.types import Scope

app_logger = logging.getLogger("yali.appserv.portal")


async def after_exception_hook(exc: Exception, scope: Scope) -> None:
    state = scope["app"].state

    if not hasattr(state, "error_count"):
        state.error_count = 1
    else:
        state.error_count += 1

    app_logger.info(
        "An exception of type %s has occurred for requested path %s and the application error count is %d.",
        type(exc).__name__,
        scope["path"],
        state.error_count,
    )
