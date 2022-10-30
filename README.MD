nonebot-plugin-sqlalchemy
==========

为nonebot2提供简单的sqlalchemy封装

## Get Started

```python
# 【1】定义data_source
from nonebot import get_driver, require

# 注意必须先require再import
require("nonebot_plugin_sqlalchemy")
from nonebot_plugin_sqlalchemy import DataSource

# 必须使用支持asyncio的驱动器
db_conn_url = "postgresql+asyncpg://username:password@localhost:5432/database"
data_source = DataSource(get_driver(), db_conn_url)


# 【2】定义映射
from sqlalchemy import Column, String, BigInteger, Integer

@data_source.registry.mapped
class UserOrm:
    __tablename__ = 'users'

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    username: str = Column(String)
    password: str = Column(String)
    nickname: str = Column(String)

    
# 【3】在nonebot中使用
from nonebot import on_command
from nonebot.adapters.onebot.v11 import MessageEvent
from nonebot.internal.matcher import Matcher
from sqlalchemy import select

login_matcher = on_command("login")

@login_matcher.handle()
async def handler(event: MessageEvent, matcher: Matcher):
    username, password = event.get_plaintext().split(" ")
    
    # 此处获取的session实际上是async_scoped_session
    # 在Matcher的一次执行过程中，多次调用data_source.session()获得的是同一个session，并且会在Matcher执行完毕后自动关闭
    # 参考：https://docs.sqlalchemy.org/en/14/orm/extensions/asyncio.html#using-asyncio-scoped-session
    session = data_source.session()
    
    stmt = select(UserOrm).where(UserOrm.username == username, UserOrm.password == password)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()

    if user is not None:
        await matcher.send(f"Hello, {user.nickname}")
```
