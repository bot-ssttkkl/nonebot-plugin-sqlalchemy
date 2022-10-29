import asyncio

from nonebot import logger, Driver
from nonebot.message import run_postprocessor
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_scoped_session
from sqlalchemy.orm import registry
from sqlalchemy.orm import sessionmaker


class DataSourceNotReadyError(RuntimeError):
    pass


class DataSource:
    def __init__(self, driver: Driver, url: str, **kwargs):
        self._engine = None
        self._session = None

        self._registry = registry()

        # 仅当debug模式时回显sql语句
        kwargs.setdefault("echo", driver.config.log_level.lower() == 'debug')
        kwargs.setdefault("future", True)

        @driver.on_startup
        async def on_startup():
            self._engine = create_async_engine(url, **kwargs)

            async with self._engine.begin() as conn:
                await conn.run_sync(self._registry.metadata.create_all)

            # expire_on_commit=False will prevent attributes from being expired
            # after commit.
            session_factory = sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
            self._session = async_scoped_session(
                session_factory, scopefunc=asyncio.current_task)
            logger.success("Succeeded to initialize data source")

        @driver.on_shutdown
        async def on_shutdown():
            await self._engine.dispose()

            self._engine = None
            self._session = None

            logger.success("Succeeded to dispose data source")

        @run_postprocessor
        async def remove_session():
            if self._session is not None:
                await self._session.close()
                await self._session.remove()

    @property
    def registry(self) -> registry:
        return self._registry

    def session(self) -> AsyncSession:
        if self._session is None:
            raise DataSourceNotReadyError()
        return self._session()


__all__ = ("DataSource", "DataSourceNotReadyError")
