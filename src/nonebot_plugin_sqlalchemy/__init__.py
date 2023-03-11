from asyncio import gather
from inspect import isawaitable
from typing import Callable, Awaitable, Union

from nonebot import logger, Driver
from nonebot.internal.matcher import current_matcher
from nonebot.message import run_postprocessor
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine, create_async_engine, async_scoped_session
from sqlalchemy.orm import registry
from sqlalchemy.orm import sessionmaker

T_OnReadyCallback = Union[Callable[[], None], Callable[[], Awaitable[None]]]

T_OnEngineCreatedCallback = Union[Callable[[AsyncEngine], None], Callable[[AsyncEngine], Awaitable[None]]]


async def _fire(callbacks, args=None, kwargs=None):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}

    coros = [f(*args, **kwargs) for f in callbacks]
    coros = [coro for coro in coros if isawaitable(coro)]
    await gather(*coros)


class DataSourceNotReadyError(RuntimeError):
    pass


class DataSource:
    def __init__(self, driver: Driver, url: str, **kwargs):
        self._engine = None
        self._session = None

        self._registry = registry()

        self._on_engine_created_callback = []
        self._on_ready_callback = []

        # 仅当trace模式时回显sql语句
        kwargs.setdefault("echo", driver.config.log_level.lower() == 'TRACE')
        kwargs.setdefault("future", True)

        @driver.on_startup
        async def on_startup():
            self._engine = create_async_engine(url, **kwargs)
            await _fire(self._on_engine_created_callback)

            async with self._engine.begin() as conn:
                await conn.run_sync(self._registry.metadata.create_all)

            # expire_on_commit=False will prevent attributes from being expired
            # after commit.
            session_factory = sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
            self._session = async_scoped_session(
                session_factory, scopefunc=current_matcher.get)

            await _fire(self._on_ready_callback)
            logger.success("data source initialized")

        @driver.on_shutdown
        async def on_shutdown():
            await self._engine.dispose()

            self._engine = None
            self._session = None

            logger.success("data source disposed")

        @run_postprocessor
        async def postprocessor():
            if self._session is not None:
                await self._session.remove()
                logger.trace("session removed")

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

    def on_engine_created(self, action: T_OnEngineCreatedCallback):
        logger.trace("engine created")
        self._on_engine_created_callback.append(action)

    def on_ready(self, action: T_OnReadyCallback):
        logger.trace("ready")
        self._on_ready_callback.append(action)


__all__ = ("DataSource", "DataSourceNotReadyError")
