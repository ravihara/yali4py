import base64
import os
from http import HTTPStatus
from typing import Any, Callable, Dict, List

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from msgspec import DecodeError, ValidationError
from msgspec.structs import asdict as json_dict

from ..models import BaseModel
from .osfiles import FSNode
from .timings import Chrono

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


class JWKSEntry(BaseModel):
    kty: str
    alg: str
    use: str
    kid: str
    n: str | None
    e: str | None
    x: str | None
    y: str | None
    crv: str | None
    x5t: str
    x5c: List[str]


class JWTFailure(BaseModel):
    status: HTTPStatus
    reason: str


JWTPayloadValidator = Callable[[JWTPayload], JWTFailure | None]


class JWTNode:
    @staticmethod
    def jwks_to_pem(jwks: JWKSEntry):
        if jwks.kty == "RSA":
            public_numbers = rsa.RSAPublicNumbers(
                e=int.from_bytes(base64.urlsafe_b64decode(jwks.e + "=="), "big"),
                n=int.from_bytes(base64.urlsafe_b64decode(jwks.n + "=="), "big"),
            )
            public_key = public_numbers.public_key()
        elif jwks.kty == "EC":
            curve = {
                "P-256": ec.SECP256R1(),
                "P-384": ec.SECP384R1(),
                "P-521": ec.SECP521R1(),
            }[jwks.crv]
            public_key = ec.EllipticCurvePublicNumbers(
                x=int.from_bytes(base64.urlsafe_b64decode(jwks.x + "=="), "big"),
                y=int.from_bytes(base64.urlsafe_b64decode(jwks.y + "=="), "big"),
                curve=curve,
            ).public_key()
        else:
            raise ValueError(f"Unsupported key type: {jwks.kty}")

        pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        return pem.decode("utf-8")

    @staticmethod
    def signing_key_from_env():
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

        if not FSNode.is_file_readable(key_file):
            raise ValueError(f"JWT_SIGNING_KEY_FILE '{key_file}' is not readable")

        with open(key_file, "r") as f:
            _yali_jwt_signing_key = f.read()

        return _yali_jwt_signing_key

    @staticmethod
    def generate_token(payload: JWTPayload) -> str:
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
        signing_key = JWTNode.signing_key_from_env()
        ws_jwt = jwt.encode(json_dict(payload), signing_key, algorithm="HS256")

        return ws_jwt

    @staticmethod
    def verify_reference(
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
        signing_key = JWTNode.signing_key_from_env()
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

        try:
            payload_dict = jwt.decode(
                jwt=jwt_token,
                key=signing_key,
                algorithms=["HS256"],
                options=verify_opts,
            )
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

            now = Chrono.get_current_utc_time().timestamp()

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
