from http import HTTPStatus
from logging import Logger
from typing import List, Literal

from websockets.asyncio.server import Request as AioWsRequest
from websockets.asyncio.server import Response as AioWsResponse
from websockets.asyncio.server import ServerConnection as AioWsServerConnection
from yali.auth import (
    JWTFailure,
    JWTPayloadValidator,
    JWTReference,
    verify_jwt_reference,
)

YaliWsClientType = Literal["UNI_TXN_WS_CLIENT", "LOOPED_WS_CLIENT"]
YaliWsClients: List[YaliWsClientType] = [
    "UNI_TXN_WS_CLIENT",
    "LOOPED_WS_CLIENT",
]


def validate_ws_client(client_id: str):
    """
    Check if a client is a valid Yali defined Websocket client based on the client id

    Parameters
    ----------
    client_id : str
        The client id to be checked

    Returns
    -------
    bool
        True if the client is a valid Yali defined Websocket client, False otherwise
    """
    id_parts = client_id.split("|")

    if len(id_parts) != 2:
        return False

    return id_parts[0] in YaliWsClients


def wrap_server_process_request(
    logger: Logger,
    with_jwt_auth: bool = False,
    jwt_reference: JWTReference | None = None,
    jwt_validator: JWTPayloadValidator | None = None,
):
    """
    The ws-server process request wrapper, providing JWT and other goodies.
    """

    async def process_request(
        connection: AioWsServerConnection,
        request: AioWsRequest,
    ) -> AioWsResponse | None:
        try:
            client_id = request.headers["X-Client-Id"]

            if not validate_ws_client(client_id):
                response = connection.respond(
                    HTTPStatus.UNAUTHORIZED,
                    "Invalid X-Client-Id header\n",
                )

                return response
        except KeyError:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Missing X-Client-Id header\n",
            )

            return response

        if not with_jwt_auth:
            connection.username = client_id
            return None

        if not jwt_reference:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Missing JWT reference to validate the payload\n",
            )

            return response

        try:
            auth_header = request.headers["Authorization"]

            if not auth_header.startswith("Bearer "):
                response = connection.respond(
                    HTTPStatus.UNAUTHORIZED,
                    "Invalid Authorization header\n",
                )

                return response

            jwt_token = auth_header[7:]
            response = verify_jwt_reference(
                jwt_token=jwt_token,
                jwt_reference=jwt_reference,
                jwt_validator=jwt_validator,
            )

            if isinstance(response, JWTFailure):
                response = connection.respond(
                    response.status,
                    response.reason,
                )

                return response

            logger.debug(f"JWT payload: {response.model_dump_json()}")
        except KeyError:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Missing Authorization header\n",
            )

            return response
        except ValueError:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Internal error while fetching signing key\n",
            )

            return response
        except Exception as ex:
            logger.error("Failed to verify Authorization header", exc_info=ex)

            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Internal error while verifying Authorization header\n",
            )

            return response

        connection.username = client_id
        return None

    return process_request
