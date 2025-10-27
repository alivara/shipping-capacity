from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from app.config import get_settings

settings = get_settings()


def setup_middleware(app: FastAPI) -> None:
    """Setup middleware for the FastAPI application."""

    app.add_middleware(TrustedHostMiddleware, allowed_hosts=settings.BASE.TRUSTED_HOSTS)
    app.add_middleware(
        CORSMiddleware,
        allow_credentials=True,
        allow_methods=["DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT"],
        allow_origins=settings.BASE.ALLOWED_ORIGINS,
        allow_headers=[
            "accept",
            "accept-encoding",
            "authorization",
            "content-type",
            "dnt",
            "origin",
            "user-agent",
            "x-csrftoken",
            "x-requested-with",
            "x-secret",
        ],
    )
