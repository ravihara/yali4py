import os
import ssl

from .appconf.ssl import ClientSSLConfig, ServerSSLConfig

_def_client_ssl_settings = ClientSSLConfig()
_def_server_ssl_settings = ServerSSLConfig()


class SSLNode:
    @staticmethod
    def create_context(
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

    @staticmethod
    def server_context(settings: ServerSSLConfig = _def_server_ssl_settings):
        """
        Get the SSL context for the server using environment variables

        Returns
        -------
        ssl.SSLContext
            SSL context
        """

        verify_mode = (
            ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
        )
        get_password = (
            (lambda: settings.password.get_secret_value())
            if settings.password
            else None
        )

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_default_certs(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

        ctx.verify_mode = ssl.VerifyMode(verify_mode)

        if not settings.is_self_signed:
            ctx.load_verify_locations(settings.ca_file)

        return ctx

    @staticmethod
    def client_context(settings: ClientSSLConfig = _def_client_ssl_settings):
        """
        Get the SSL context for the client using environment variables

        Returns
        -------
        ssl.SSLContext
            SSL context
        """

        verify_mode = (
            ssl.CERT_OPTIONAL if settings.is_self_signed else ssl.CERT_REQUIRED
        )
        get_password = (
            (lambda: settings.password.get_secret_value())
            if settings.password
            else None
        )

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        ctx.load_cert_chain(settings.cert_file, settings.key_file, get_password)

        ctx.verify_mode = ssl.VerifyMode(verify_mode)

        if not settings.is_self_signed:
            ctx.load_verify_locations(settings.ca_file)

        return ctx
