import asyncio
import logging
from typing import Annotated, Any, Callable, Coroutine, Dict
from uuid import uuid4

from msgspec import DecodeError, ValidationError
from websockets import exceptions as ws_exc
from websockets.asyncio.client import ClientConnection as AioWsClientConnection
from websockets.asyncio.client import connect as aio_ws_connect
from websockets.frames import CloseCode

from yali.core.codecs import JSONNode
from yali.core.common import dict_to_result
from yali.core.models import BaseModel, Failure, Result, Success, field_specs
from yali.core.secjwt import JWTNode, JWTPayload
from yali.core.secssl import SSLNode
from yali.core.typebase import ConstrNode, WebsocketUrl

WsClientExcludeArgs = [
    "ws_url",
    "jwt_payload",
    "on_message",
    "retry_timeout_sec",
    "ssl",
    "logger",
]


def _update_headers(
    kwargs: Dict[str, Any], client_id: str, jwt_payload: JWTPayload | None = None
):
    if "additional_headers" in kwargs:
        if isinstance(kwargs["additional_headers"], list):
            kwargs["additional_headers"].append(("X-Client-Id", client_id))
        else:
            kwargs["additional_headers"]["X-Client-Id"] = client_id
    else:
        kwargs["additional_headers"] = [("X-Client-Id", client_id)]

    if jwt_payload:
        jwt = JWTNode.generate_token(jwt_payload)

        if isinstance(kwargs["additional_headers"], list):
            kwargs["additional_headers"].append(("Authorization", f"Bearer {jwt}"))
        else:
            kwargs["additional_headers"]["Authorization"] = f"Bearer {jwt}"


class LoopedWsClientConfig(BaseModel):
    ws_url: WebsocketUrl
    on_message: Callable[[AioWsClientConnection], Coroutine[Any, Any, None]]
    jwt_payload: JWTPayload | None = None
    retry_timeout_sec: Annotated[float, ConstrNode.constr_num(ge=10.0)] = field_specs(
        default=10.0
    )


class UniTxnWsClient:
    _logger = logging.getLogger(__name__)

    def __init__(
        self, *, ws_url: WebsocketUrl, jwt_payload: JWTPayload | None = None, **kwargs
    ) -> None:
        self._url = str(ws_url)
        self._cid = f"UNI_TXN_WS_CLIENT|{uuid4()}"
        self._jwt_payload = jwt_payload

        if not kwargs:
            self._kwargs = {}
        else:
            for k in WsClientExcludeArgs:
                if k in kwargs:
                    del kwargs[k]

            self._kwargs = kwargs

        _update_headers(self._kwargs, self._cid, self._jwt_payload)

        if ws_url.startswith("wss://"):
            self._ssl_context = SSLNode.client_context()
        else:
            self._ssl_context = None
            self._logger.warning(
                f"Websocket URL {self._url} is not secure. It is recommended to use 'wss' scheme"
            )

    async def _handle_connection(
        self, connection: AioWsClientConnection, data: Dict[str, Any]
    ):
        try:
            await connection.send(JSONNode.dump_str(data))

            response = await connection.recv()
            response = JSONNode.safe_load_data(data=response)

            if not isinstance(response, dict):
                return Success(data={"result": response})

            response = dict_to_result(response)
            return response
        except (ValidationError, DecodeError) as ex:
            self._logger.error(ex, exc_info=True)
            response = Failure(
                error="Invalid response from server",
                extra={"exc": str(ex)},
            )
            return response
        except Exception as ex:
            self._logger.error(ex, exc_info=True)
            response = Failure(
                error="Internal error from server", extra={"exc": str(ex)}
            )
            return response

    async def execute(self, data: Dict[str, Any]) -> Result:
        try:
            async with aio_ws_connect(
                self._url, ssl=self._ssl_context, logger=self._logger, **self._kwargs
            ) as connection:
                self._logger.info(f"The ws-client {self._cid} connected to {self._url}")
                response = await self._handle_connection(connection, data)

                return response
        except (ws_exc.ConnectionClosed, ws_exc.ConnectionClosedError) as ex:
            return Failure(
                error=f"Connection closed for ws-client {self._cid} to {self._url} with error {ex.reason}"
            )
        except (ConnectionAbortedError, ConnectionRefusedError) as ex:
            self._logger.error(ex, exc_info=True)
            return Failure(
                error=f"Connection aborted or refused for ws-client {self._cid} to {self._url}"
            )
        except ws_exc.InvalidURI:
            return Failure(
                error=f"Invalid URI for ws-client {self._cid} to {self._url}"
            )
        except KeyboardInterrupt:
            return Failure(
                error=f"KeyboardInterrupt for ws-client {self._cid} to {self._url}"
            )
        except Exception as ex:
            self._logger.error(ex, exc_info=True)
            return Failure(
                error=f"Internal error for ws-client {self._cid} to {self._url}"
            )


class LoopedWsClient:
    _logger = logging.getLogger(__name__)

    def __init__(self, *, config: LoopedWsClientConfig, **kwargs) -> None:
        self.__is_running = False
        self.__connection: AioWsClientConnection | None = None

        self._config = config
        self._cid = f"LOOPED_WS_CLIENT|{uuid4()}"

        if not kwargs:
            self._kwargs = {}
        else:
            for k in WsClientExcludeArgs:
                if k in kwargs:
                    del kwargs[k]

            self._kwargs = kwargs

        _update_headers(self._kwargs, self._cid, self._config.jwt_payload)

        if config.ws_url.startswith("wss://"):
            self._ssl_context = SSLNode.client_context()
        else:
            self._ssl_context = None
            self._logger.warning(
                f"Websocket URL {config.ws_url} is not secure. It is recommended to use 'wss' scheme"
            )

    @property
    def is_running(self):
        return self.__is_running

    async def _safe_close(self, reason: str):
        try:
            if self.__connection:
                await self.__connection.close(
                    code=CloseCode.TRY_AGAIN_LATER, reason=reason
                )
        except Exception:
            pass

        self.__connection = None
        self.__is_running = False

    async def start(self):
        if self.__is_running:
            self._logger.warning(f"Looped ws-client {self._cid} is already running")
            return

        self._logger.info(
            f"Establishing looped ws-client {self._cid} connection to {self._config.ws_url}"
        )

        self.__is_running = True
        closing_code: CloseCode = CloseCode.NORMAL_CLOSURE
        ws_url = str(self._config.ws_url)
        retry_sec = self._config.retry_timeout_sec

        while self.__is_running:
            try:
                async with aio_ws_connect(
                    ws_url, ssl=self._ssl_context, logger=self._logger, **self._kwargs
                ) as connection:
                    self._logger.info(
                        f"The ws-client {self._cid} connected to {ws_url}"
                    )
                    self.__connection = connection

                    await self._config.on_message(connection)
            except (ws_exc.ConnectionClosed, ws_exc.ConnectionClosedError) as ex:
                self.__connection = None
                self._logger.warning(
                    f"Connection closed for ws-client {self._cid} to {ws_url} with error {ex.reason}. Retrying after {retry_sec} seconds"
                )
                await asyncio.sleep(retry_sec)
            except (ConnectionAbortedError, ConnectionRefusedError) as ex:
                self.__connection = None
                self._logger.warning(
                    f"Connection aborted or refused for ws-client {self._cid} to {ws_url}. Retrying after {retry_sec} seconds",
                    exc_info=ex,
                )
                await asyncio.sleep(retry_sec)
            except ws_exc.InvalidURI:
                self.__connection = None
                closing_code = CloseCode.POLICY_VIOLATION
                self._logger.error(f"Invalid URI for ws-client {self._cid} to {ws_url}")
                break
            except KeyboardInterrupt:
                self.__connection = None
                closing_code = CloseCode.ABNORMAL_CLOSURE
                self._logger.warning(
                    f"KeyboardInterrupt for ws-client {self._cid} to {ws_url}"
                )
                break
            except Exception as ex:
                self._logger.error(ex, exc_info=True)
                await self._safe_close(reason=str(ex))
                await asyncio.sleep(retry_sec)

        await self.stop(code=closing_code)

    async def stop(self, code: CloseCode = CloseCode.NORMAL_CLOSURE):
        self.__is_running = False

        if self.__connection:
            try:
                await self.__connection.close(code=code, reason="Stopped")
                await self.__connection.wait_closed()
            except ws_exc.ConnectionClosed:
                self._logger.warning(
                    f"Connection already closed for ws-client {self._cid}"
                )

        self.__connection = None
