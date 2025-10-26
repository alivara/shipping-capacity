from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import Environment, get_settings

settings = get_settings()
engine = create_async_engine(
    f"{settings.DATABASE.POSTGRESQL_URL_ASYNC}",
    echo=True if settings.BASE.ENV == Environment.LOCAL else False,
)

async_session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def get_session():
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with get_session() as session:
        yield session
