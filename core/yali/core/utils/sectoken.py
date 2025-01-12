import os
import ssl
from http import HTTPStatus
from typing import Any, Callable, Dict, List

import jwt
from msgspec import DecodeError, ValidationError
from msgspec.structs import asdict as json_dict

from ..models import BaseModel
from ..settings import ClientSSLSettings, ServerSSLSettings
from ..utils.datetimes import DateTimeConv
from ..utils.osfiles import FilesConv

_yali_jwt_signing_key: str | None = None


class JWTReference(BaseModel):
    issuers: List[str]
    audience: List[str]
    subject: str
    leeway: float = 0.0


class JWTPayload(BaseModel):
    iss: str
    aud: str
    sub: str
    iat: float  ## IssuedAt Datetime in unix timestamp
    exp: float  ## Expiry Datetime in unix timestamp
    nbf: float | None = None  ## NotBefore Datetime in unix timestamp
    custom: Dict[str, Any] = {}


class JWTFailure(BaseModel):
    status: HTTPStatus
    reason: str


JWTPayloadValidator = Callable[[JWTPayload], JWTFailure | None]


def create_ssl_context(
    certfile: str | os.PathLike[str],
    keyfile: str | os.PathLike[str] | None,
    password: str | None,
    ssl_version: int,
    cert_reqs: int,
    ca_certs: str | os.PathLike[str] | None,
    ciphers: str | None,
) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl_version)
    get_password = (lambda: password) if password else None

    ctx.load_cert_chain(certfile, keyfile, get_password)
    ctx.verify_mode = ssl.VerifyMode(cert_reqs)

    if ca_certs:
        ctx.load_verify_locations(ca_certs)

    if ciphers:
        ctx.set_ciphers(ciphers)

    return ctx


def server_ssl_context():
    """
    Get the SSL context for the server using environment variables

    Returns
    -------
    ssl.SSLContext
        SSL context
    """

    settings = ServerSSLSettings()
    verify_mode = ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
    get_password = (
        (lambda: settings.password.get_secret_value()) if settings.password else None
    )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_default_certs(ssl.Purpose.CLIENT_AUTH)
    ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

    ctx.verify_mode = ssl.VerifyMode(verify_mode)

    if not settings.is_self_signed:
        ctx.load_verify_locations(settings.ca_file)

    return ctx


def client_ssl_context():
    """
    Get the SSL context for the client using environment variables

    Returns
    -------
    ssl.SSLContext
        SSL context
    """
    settings = ClientSSLSettings()
    verify_mode = ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
    get_password = (
        (lambda: settings.password.get_secret_value()) if settings.password else None
    )

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

    ctx.verify_mode = ssl.VerifyMode(verify_mode)

    if not settings.is_self_signed:
        ctx.load_verify_locations(settings.ca_file)

    return ctx


def jwt_signing_key_from_env():
    """
    Get the PEM formatted signing key from the environment variable JWT_SIGNING_KEY_FILE.

    Returns
    -------
    str
        PEM formatted signing key
    """
    global _yali_jwt_signing_key

    if _yali_jwt_signing_key:
        return _yali_jwt_signing_key

    key_file = os.getenv("JWT_SIGNING_KEY_FILE")

    if not key_file:
        raise ValueError("JWT_SIGNING_KEY_FILE is not set")

    if not FilesConv.is_file_readable(key_file):
        raise ValueError(f"JWT_SIGNING_KEY_FILE '{key_file}' is not readable")

    with open(key_file, "r") as f:
        _yali_jwt_signing_key = f.read()

    return _yali_jwt_signing_key


def generate_jwt(payload: JWTPayload) -> str:
    """
    Generate a JWT token from the provided JWTPayload. This uses HS256 algorithm
    and PEM formatted signing key.

    Parameters
    ----------
    payload: JWTPayload
        JWTPayload to be used for generating JWT token

    Returns
    -------
    str
        JWT token
    """
    signing_key = jwt_signing_key_from_env()
    ws_jwt = jwt.encode(json_dict(payload), signing_key, algorithm="HS256")

    return ws_jwt


def verify_jwt_reference(
    *,
    jwt_token: str,
    jwt_reference: JWTReference,
    jwt_validator: JWTPayloadValidator | None = None,
):
    """
    Verify expected claims in the JWT token using the JWT Reference, provided.
    An optional, custom validator can be provided to further validate the JWT payload.
    This facilitates extending the JWT payload with custom claims.

    Parameters
    ----------
    jwt_token: str
        JWT token to be verified
    jwt_reference: JWTReference
        JWT Reference to be used for verification
    jwt_validator: JWTPayloadValidator | None
        Optional, custom validator to be applied to the JWT payload

    Returns
    -------
    JWTPayload | JWTFailure
        JWTPayload if the JWT token is valid, JWTFailure otherwise
    """
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

        if jwt_validator:
            failure = jwt_validator(jwt_payload)

            if failure:
                return failure

        return jwt_payload
    except (ValidationError, DecodeError):
        return JWTFailure(
            status=HTTPStatus.UNAUTHORIZED,
            reason="Invalid JWT Payload received\n",
        )
