import asyncio
import json
import logging
from ssl import SSLContext
from typing import Any, Callable, Coroutine, Dict

from websockets.asyncio.server import Server as AioWsServer
from websockets.asyncio.server import serve as aio_ws_serve
from websockets.exceptions import ConnectionClosed as WsConnectionClosed
from websockets.exceptions import ConnectionClosedError as WsConnectionClosedError
from websockets.exceptions import ConnectionClosedOK as WsConnectionClosedOK
from websockets.frames import CloseCode
from yali.core.typings import Failure, Field, FlexiTypesModel, Result

from .protodef import (
    AioWsServerConnection,
    JWTValidator,
    ensure_ws_client,
    server_ssl_context,
)


class YaliWsServerConfig(FlexiTypesModel):
    service: str = Field(min_length=3)
    host: str = Field(min_length=3)
    port: int
    with_ssl: bool = False
    with_jwt_auth: bool = False
    on_connect: Callable[[AioWsServerConnection, str], Coroutine[Any, Any, Result]]
    on_message: Callable[[str, str, Dict], Coroutine[Any, Any, None]]
    on_disconnect: Callable[[AioWsServerConnection, str], Coroutine[Any, Any, None]]
    jwt_validator: JWTValidator = None


WsServerExcludeArgs = [
    "service",
    "host",
    "port",
    "with_jwt_auth",
    "on_connect",
    "on_message",
    "on_disconnect",
    "handler",
    "process_request",
    "logger",
    "start_serving",
    "ssl",
    "jwt_validator",
]


class YaliWebSocketServer:
    def __init__(self, config: YaliWsServerConfig, **kwargs):
        self.__instance: AioWsServer | None = None

        self._config = config
        self._logger = logging.getLogger(config.service)

        ## Ensure to remove internally handled args
        for k in WsServerExcludeArgs:
            if k in kwargs:
                del kwargs[k]

        self._kwargs = kwargs

    async def _handle_connection(self, connection: AioWsServerConnection):
        assert connection.request

        client_id = connection.username
        request_path = connection.request.path
        connect_res = await self._config.on_connect(connection, request_path)

        if connect_res.tid == "fl":
            self._logger.error(connect_res.error)
            await connection.close(code=CloseCode.INTERNAL_ERROR, reason=connect_res.error)
            return

        self._logger.debug(connect_res.data)
        self._logger.info(
            f"The {self._config.service} ws-server connected with client {client_id} at {request_path}"
        )

        try:
            async for message in connection:
                try:
                    mesg_json: Dict = json.loads(message)
                    await self._config.on_message(client_id, request_path, mesg_json)
                except json.JSONDecodeError as ex:
                    self._logger.error(ex, exc_info=True)
                    error_res = Failure(error=str(ex))
                    await connection.send(error_res.model_dump_json())
                except WsConnectionClosed:
                    self._logger.warning(
                        f"Cannot use already closed {self._config.service} ws-server connection for client {client_id} at {request_path}"
                    )
                    await self._config.on_disconnect(connection, request_path)
                    break
                except WsConnectionClosedError as ex:
                    self._logger.error(
                        f"The {self._config.service} ws-server connection for client {client_id} at {request_path} closed with error {ex.reason}"
                    )
                    await self._config.on_disconnect(connection, request_path)
                    break
                except WsConnectionClosedOK:
                    self._logger.info(
                        f"The {self._config.service} ws-server connection for client {client_id} at {request_path} closed normally"
                    )
                    await self._config.on_disconnect(connection, request_path)
                    break
                except Exception as ex:
                    self._logger.error(ex, exc_info=True)
                    await asyncio.sleep(0)
        except WsConnectionClosed:
            self._logger.warning(
                f"Cannot use already closed {self._config.service} ws-server (no-message) connection for client {client_id} at {request_path}"
            )
            await self._config.on_disconnect(connection, request_path)
        except WsConnectionClosedError as ex:
            self._logger.error(
                f"The {self._config.service} ws-server (no-message) connection for client {client_id} at {request_path} closed with error {ex.reason}"
            )
            await self._config.on_disconnect(connection, request_path)
        except WsConnectionClosedOK:
            self._logger.info(
                f"The {self._config.service} ws-server (no-message) connection for client {client_id} at {request_path} closed normally"
            )
            await self._config.on_disconnect(connection, request_path)

    async def start(self):
        self._logger.info(
            f"Starting {self._config.service} server on {self._config.host}:{self._config.port}"
        )

        if self.__instance and self.__instance.is_serving():
            self._logger.warning(f"Server {self._config.service} is already running")
            return

        ssl_context: SSLContext | None = None

        if self._config.with_ssl:
            try:
                ssl_context = server_ssl_context()
            except ValueError as ex:
                self._logger.error(ex, exc_info=True)
                return

        process_request = ensure_ws_client(
            logger=self._logger,
            with_jwt_auth=self._config.with_jwt_auth,
            jwt_validator=self._config.jwt_validator,
        )

        self.__instance = await aio_ws_serve(
            handler=self._handle_connection,
            host=self._config.host,
            port=self._config.port,
            logger=self._logger,
            process_request=process_request,
            start_serving=False,
            ssl=ssl_context,
            **self._kwargs,
        )

        try:
            self._logger.info(f"Server {self._config.service} started")
            await self.__instance.start_serving()
        except asyncio.CancelledError:
            self._logger.info(f"Server {self._config.service} connection cancelled")

        await self.stop()

    async def stop(self):
        if self.__instance and self.__instance.is_serving():
            self.__instance.close(close_connections=True)
            await self.__instance.wait_closed()
            self._logger.info(f"Server {self._config.service} stopped")

        self.__instance = None
