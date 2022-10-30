from nonebot import logger, Driver
from nonebot.internal.matcher import Matcher, current_matcher
from nonebot.message import run_postprocessor
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine, async_scoped_session
from sqlalchemy.orm import registry
from sqlalchemy.orm import sessionmaker

from .hack_func import hack_func


class DataSourceNotReadyError(RuntimeError):
    pass


class DataSource:
    def __init__(self, driver: Driver, url: str, **kwargs):
        self._engine = None
        self._session = None

        self._registry = registry()

        # 因为nonebot执行run_postprocessor时已经把current_matcher给reset了，所以需要hack
        # 并且也不能用asyncio.current_task作为scopefunc，因为run_postprocessor是在独立的Task中执行
        self._scopefunc = hack_func(current_matcher.get)

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
                session_factory, scopefunc=self._scopefunc)
            logger.success("data source initialized")

        @driver.on_shutdown
        async def on_shutdown():
            await self._engine.dispose()

            self._engine = None
            self._session = None

            logger.success("data source disposed")

        @run_postprocessor
        async def postprocessor(matcher: Matcher):
            hack_t = self._scopefunc.hack.set(matcher)
            try:
                if self._session is not None:
                    await self._session.close()
                    await self._session.remove()
                    logger.success("session removed")
            finally:
                self._scopefunc.hack.reset(hack_t)

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise DataSourceNotReadyError()
        return self._engine

    @property
    def registry(self) -> registry:
        return self._registry

    @property
    def session(self) -> async_scoped_session:
        if self._session is None:
            raise DataSourceNotReadyError()
        return self._session


__all__ = ("DataSource", "DataSourceNotReadyError")
