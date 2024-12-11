import os
import ssl
from http import HTTPStatus
from typing import List

import jwt
from pydantic import ValidationError
from yali.core.typings import FlexiTypesModel
from yali.core.utils.datetimes import DateTimeConv
from yali.core.utils.osfiles import FilesConv

_yali_jwt_signing_key: str | None = None


class JWTReference(FlexiTypesModel):
    issuers: List[str]
    audience: List[str]
    subject: str
    leeway: float = 0.0


class JWTPayload(FlexiTypesModel):
    iss: str
    aud: str
    sub: str
    iat: float  ## IssuedAt Datetime in unix timestamp
    exp: float  ## Expiry Datetime in unix timestamp


class JWTFailure(FlexiTypesModel):
    status: HTTPStatus
    reason: str


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


def verify_jwt_reference(jwt_token: str, jwt_reference: JWTReference):
    signing_key = jwt_signing_key_from_env()
    verify_opts = {
        "verify_signature": True,
        "verify_exp": False,
        "verify_nbf": False,
        "verify_iat": False,
        "verify_aud": False,
        "verify_iss": False,
        "verify_sub": False,
        "verify_jti": False,
        "require": [],
    }

    payload_dict = jwt.decode(
        jwt=jwt_token,
        key=signing_key,
        algorithms=["HS256"],
        options=verify_opts,
    )

    try:
        jwt_payload = JWTPayload(**payload_dict)

        if jwt_payload.iss not in jwt_reference.issuers:
            return JWTFailure(
                status=HTTPStatus.UNAUTHORIZED,
                reason="Invalid JWT Issuer received\n",
            )

        if jwt_payload.aud not in jwt_reference.audience:
            return JWTFailure(
                status=HTTPStatus.UNAUTHORIZED,
                reason="Invalid JWT Audience received\n",
            )

        if jwt_payload.sub != jwt_reference.subject:
            return JWTFailure(
                status=HTTPStatus.UNAUTHORIZED,
                reason="Invalid JWT Subject received\n",
            )

        now = DateTimeConv.get_current_utc_time().timestamp()

        if int(jwt_payload.exp) <= (now - jwt_reference.leeway):
            return JWTFailure(
                status=HTTPStatus.UNAUTHORIZED,
                reason="JWT payload is expired\n",
            )

        if int(jwt_payload.iat) > (now + jwt_reference.leeway):
            return JWTFailure(
                status=HTTPStatus.UNAUTHORIZED,
                reason="JWT payload is not yet valid\n",
            )

        return jwt_payload
    except ValidationError:
        return JWTFailure(
            status=HTTPStatus.UNAUTHORIZED,
            reason="Invalid JWT Payload received\n",
        )
