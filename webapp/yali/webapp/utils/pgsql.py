from typing import cast

from litestar import Litestar
from litestar.params import Parameter
from litestar.plugins.sqlalchemy import (
    AsyncSessionConfig,
    SQLAlchemyAsyncConfig,
    SQLAlchemyPlugin,
    async_default_before_send_handler,
    filters,
)
from sqlalchemy import URL as DbUrl
from sqlalchemy.ext.asyncio import AsyncEngine

from ..settings import PgSQLSettings
from .sqldb import DbModel

__settings = PgSQLSettings()
__db_url = DbUrl.create(
    drivername="postgresql+asyncpg",
    username=__settings.username,
    password=__settings.password,
    host=__settings.host,
    port=__settings.port,
    database=__settings.database,
)

## SQLAlchemy specific exports for postgresql
db_session_config = AsyncSessionConfig(expire_on_commit=False)
db_config = SQLAlchemyAsyncConfig(
    connection_string=__db_url.render_as_string(hide_password=False),
    session_config=db_session_config,
    metadata=DbModel.metadata,
    engine_dependency_key="db_engine",
    session_dependency_key="db_session",
    engine_app_state_key="db_engine",
    before_send_handler=async_default_before_send_handler,
    create_all=True,
)
db_plugin = SQLAlchemyPlugin(config=db_config)


async def session_ctx():
    return db_config.get_session()


async def connection():
    return db_config.get_engine().connect()


async def transaction_ctx():
    return db_config.get_engine().begin()


def provide_limit_offset_pagination(
    current_page: int = Parameter(ge=1, query="currentPage", default=1, required=False),
    page_size: int = Parameter(query="pageSize", ge=1, default=10, required=False),
):
    """Add offset/limit pagination.

    Return type consumed by `Repository.apply_limit_offset_pagination()`.

    Parameters
    ----------
    current_page : int
        LIMIT to apply to select.
    page_size : int
        OFFSET to apply to select.
    """
    return filters.LimitOffset(page_size, page_size * (current_page - 1))


async def app_init_db(app: Litestar):
    """App startup hook to initialize the database"""
    async with db_config.get_engine().begin() as conn:
        await conn.run_sync(DbModel.metadata.create_all())

    if not getattr(app.state, "db_engine", None):
        app.state.db_engine = db_config.get_engine()

    return cast("AsyncEngine", app.state.db_engine)


async def app_close_db(app: Litestar):
    """App shutdown hook to close the database connection"""
    if getattr(app.state, "db_engine", None):
        await cast("AsyncEngine", app.state.db_engine).dispose()
        del app.state.db_engine
