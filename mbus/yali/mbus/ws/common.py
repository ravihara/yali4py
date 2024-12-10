import os
import ssl
from http import HTTPStatus
from logging import Logger
from typing import Awaitable, Callable, List, Literal

import jwt
from websockets.asyncio.server import Request as AioWsRequest
from websockets.asyncio.server import Response as AioWsResponse
from websockets.asyncio.server import ServerConnection as AioWsServerConnection
from yali.core.typings import Field, FlexiTypesModel, NonEmptyStr
from yali.core.utils.osfiles import FilesConv

_yali_jwt_signing_key: str | None = None

YaliWsClientType = Literal["UNI_TXN_WS_CLIENT", "LOOPED_WS_CLIENT"]
YaliWsClients: List[YaliWsClientType] = [
    "UNI_TXN_WS_CLIENT",
    "LOOPED_WS_CLIENT",
]


class JWTPayload(FlexiTypesModel):
    iss: NonEmptyStr
    aud: NonEmptyStr
    sub: NonEmptyStr
    iat: int = Field(..., gt=0)
    exp: int = Field(..., ge=300)


JWTValidator = Callable[[JWTPayload], Awaitable[bool]]


def server_ssl_context():
    ssl_cert_file = os.getenv("YALI_SERVER_PEM_CERT_FILE")
    ssl_key_file = os.getenv("YALI_SERVER_PEM_KEY_FILE")

    if not ssl_cert_file or not ssl_key_file:
        raise ValueError("YALI_SERVER_PEM_CERT_FILE or YALI_SERVER_PEM_KEY_FILE is not set")

    if not FilesConv.is_file_readable(ssl_cert_file):
        raise ValueError(f"YALI_SERVER_PEM_CERT_FILE '{ssl_cert_file}' is not readable")

    if not FilesConv.is_file_readable(ssl_key_file):
        raise ValueError(f"YALI_SERVER_PEM_KEY_FILE '{ssl_key_file}' is not readable")

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_default_certs(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)

    return ssl_context


def client_ssl_context():
    ssl_cert_file = os.getenv("YALI_CLIENT_PEM_CERT_FILE")
    ssl_key_file = os.getenv("YALI_CLIENT_PEM_KEY_FILE")

    if not ssl_cert_file or not ssl_key_file:
        raise ValueError("YALI_CLIENT_PEM_CERT_FILE or YALI_CLIENT_PEM_KEY_FILE is not set")

    if not FilesConv.is_file_readable(ssl_cert_file):
        raise ValueError(f"YALI_CLIENT_PEM_CERT_FILE '{ssl_cert_file}' is not readable")

    if not FilesConv.is_file_readable(ssl_key_file):
        raise ValueError(f"YALI_CLIENT_PEM_KEY_FILE '{ssl_key_file}' is not readable")

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_default_certs(ssl.Purpose.SERVER_AUTH)
    ssl_context.load_cert_chain(certfile=ssl_cert_file, keyfile=ssl_key_file)

    return ssl_context


def jwt_signing_key_from_env():
    global _yali_jwt_signing_key

    if _yali_jwt_signing_key:
        return _yali_jwt_signing_key

    key_file = os.getenv("YALI_JWT_SIGNING_KEY_FILE")

    if not key_file:
        raise ValueError("YALI_JWT_SIGNING_KEY_FILE is not set")

    if not FilesConv.is_file_readable(key_file):
        raise ValueError(f"YALI_JWT_SIGNING_KEY_FILE '{key_file}' is not readable")

    with open(key_file, "r") as f:
        _yali_jwt_signing_key = f.read()

    return _yali_jwt_signing_key


def generate_jwt(payload: JWTPayload) -> str:
    signing_key = jwt_signing_key_from_env()
    ws_jwt = jwt.encode(payload.model_dump(), signing_key, algorithm="HS256")

    return ws_jwt


def validate_ws_client(client_id: str) -> bool:
    id_parts = client_id.split("|")

    if len(id_parts) != 2:
        return False

    return id_parts[0] in YaliWsClients


def ensure_ws_client(
    logger: Logger, with_jwt_auth: bool = False, jwt_validator: JWTValidator = None
):
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

        try:
            auth_header = request.headers["Authorization"]

            if not auth_header.startswith("Bearer "):
                response = connection.respond(
                    HTTPStatus.UNAUTHORIZED,
                    "Invalid Authorization header\n",
                )

                return response

            auth_token = auth_header[7:]
            signing_key = jwt_signing_key_from_env()
            jwt_payload = jwt.decode(
                auth_token,
                signing_key,
                algorithms=["HS256"],
            )

            logger.debug(f"Received JWT payload: {jwt_payload} for client: {client_id}")

            if jwt_validator and (not await jwt_validator(jwt_payload)):
                response = connection.respond(
                    HTTPStatus.UNAUTHORIZED,
                    "Invalid Authorization header\n",
                )

                return response
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
        except jwt.ExpiredSignatureError:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Authorization token has expired\n",
            )

            return response
        except jwt.InvalidTokenError:
            response = connection.respond(
                HTTPStatus.UNAUTHORIZED,
                "Invalid Authorization token\n",
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
