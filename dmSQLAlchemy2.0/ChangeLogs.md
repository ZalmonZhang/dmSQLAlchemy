# dmSQLAlchemy

​		此包为Python的SQLAlchemy包连接达梦数据库的适配框架，当前版本为 `2.0.17` ，API详见安装目录下的 `《DM8_dmPython使用手册》` ，目前用于适配2.0及以上版本的SQLAlchemy。

​		dmSQLAlchemy与SQLAlchemy版本并不存在一一对应的关系，2.0系列版本dmSQLAlchemy适配2.0及以上所有版本SQLAlchemy。

## ChangeLogs

#### 未发布（2026-09-23）

* 修复了异步方言 `DMDialect_dmAsync` 初始化参数错位的问题：`arraysize` 此前被静默吞掉
  （恒为 50）、`encoding_errors` 会污染 `connection_timeout` 并被传入 `dmPython.connect`，
  且 `connection_timeout` / `autocommit` 作为方言参数会报 `TypeError`；现改为关键字传参
* 修复了异步建连接时报 `attribute ... is read-only` 的问题：删除了
  `AsyncAdapt_dmasync_cursor` / `AsyncAdapt_dmasync_connection` 中对 SQLAlchemy 基类
  `__slots__` 的遮蔽声明
* 修复了 `text()` 语句配合 `executemany` 批量执行时报
  `'TextClause' object has no attribute 'table'` 的问题（同步、异步方言均受影响）
* 修复了原生 `JSON` 类型列读回为字符串而非字典的问题
* 修复了 ORM 单次提交插入多行（如 `session.add_all([...])`）报错的问题：方言此前声明支持
  "批量 + `RETURNING`"，但达梦驱动实际不支持（数组 out 变量与结果集式 `RETURNING` 均不可用），
  现不再声明该能力，由 SQLAlchemy 自动改用逐行插入，主键正确回填；同步的 `FlushError` 与
  异步的 `TypeError` 一并消除，且 `return_defaults()` + `executemany` 不再静默返回错误主键
* 修复了异步方言无法使用服务端游标的问题：异步执行上下文末尾重新定义的 `create_cursor`
  覆盖了基类 `create_cursor = default.DefaultExecutionContext.create_cursor`，导致
  `_is_server_side` 恒为 False，`AsyncConnection.stream()` 直接 `AssertionError`、
  `stream_results` 静默退化为普通游标；同时 `create_default_cursor` /
  `create_server_side_cursor` 误用裸驱动游标，现分别改用适配游标
  `self._dbapi_connection.cursor()` / `self._dbapi_connection.ss_cursor()`。修复后
  `conn.stream()` 可流式读取大结果集（实测 20 万行，事件循环最大阻塞约 0.01s）
* 修复了异步连接参数处理的三个脆弱点：`AsyncConnection._connect` 的
  `Connection(*kwargs)` 把 dict 的键当位置参数传入（kwargs ≥ 19 个即 `TypeError`），
  现改为无参构造（`Connection._connect(cargs)` 只使用 cargs 字典），并删除依赖
  `ConnectParams` 实例化的死分支；`AsyncConnection` 收了 `dsn` 却被丢弃，现保留为
  `self._dsn`，仅在无 `host` 时条件化回退使用（有 `host` 时以 host/port 为准，避免
  dmAsync 的 dsn/host 互斥 `ValueError`，使 `connect_async(dsn=...)` 真正可用）；
  `Connection._connect` 硬取 `cargs['connection_timeout']` 在 dsn-only 路径会
  `KeyError`，改为 `cargs.get('connection_timeout') or 0`

#### dmSQLAlchemy v2.0.17(2026-4-21)

* 新增了执行语句时对于多行同时插入时允许多行返回的功能
* 新增了uuid类型的支持
* 修正缓存机制，新增缓存开关控制

#### dmSQLAlchemy v2.0.16(2026-4-21)

* 新增了tuple比较运算的支持

#### dmSQLAlchemy v2.0.15(2026-2-11)

* 新增了稀疏向量、不定维向量、不定类型向量功能的支持

#### dmSQLAlchemy v2.0.14(2026-2-3)

* 修复了python3.8版本无法正常使用dmSQLAlchemy的问题

#### dmSQLAlchemy v2.0.13(2026-1-21)

* 新增了兼容milvus向量相关用法

#### dmSQLAlchemy v2.0.12(2025-12-1)

* 修复了由于文件名与包名重复时导致的引入失败的问题

#### dmSQLAlchemy v2.0.11(2025-10-21)

* 修复了在dpc环境下由于lastrowid导致的插入失败情况
* 改进了执行策略，当前将采用参数绑定的方式执行映射，执行效率将会提升

#### dmSQLAlchemy v2.0.10(2025-09-20)

* 新增了连接数据库时选择兼容模式选项
* 修复了在MySQL语法解析模式下使用limit，offset选项报错的问题

#### dmSQLAlchemy v2.0.9(2025-09-14)

* 新增了MySQL语法解析模式下对于on duplicate update功能的支持

#### dmSQLAlchemy v2.0.8(2025-8-18)

* 新增了对于达梦数据库中向量类型的支持
* 新增了在MySQL兼容模式下对于MySQL语法的兼容

#### dmSQLAlchemy v2.0.7(2025-7-13)

* 新增了对于SQLAlchemy异步功能的支持

#### dmSQLAlchemy v2.0.6(2025-06-20)

* 修正了连接错误时返回的错误码，当前连接错误时将返回`DBAPIError`
* 新增了对于inspector.get_sequence_names与inspect.get_materialized_view_names方法的支持
* 修正了inspect.get_schema_names方法无法获取所有模式名的问题
* 新增了对于JSON类型的支持

#### dmSQLAlchemy v2.0.5(2025-01-21)

* 修复了连接句柄使用 `IPV6` 格式主机名无法连接到数据库的问题

#### dmSQLAlchemy v2.0.4(2025-01-20)

* 改进了执行策略，当前获取表与序列信息将不再从 `sysobjects` 系统表获取以减少数据量，同时修改部分函数的缓存机制

* 修复了列名或表名为大小写共存的情况下，执行插入语句报错的问题
* 修复了当列名或表名为保留字的情况下，执行插入语句报错的问题
* 变更了主键策略，当前版本下，integer类型的主键将不再自动添加 `自增` 属性
* 修复了映射表时当创建者与当前使用模式不同时出现无法查询到列信息的问题

#### dmSQLAlchemy v2.0.3(2024.12.10)

* 修复了如果安装dmSQLAlchemy时没安装SQLAlchemy会安装最新版的问题
* 修复了特定情况下 `fetch` 语句拼写错误
* 修正了绑定策略，当前 `boolean` 类型将在数据库中被绑定为 `smallint` 类型
* 修复了执行多行插入时获取 `inserted_primary_key_rows` 异常的问题

#### dmSQLAlchemy v2.0.2(2024.10.31)

* 修复了部分类型无法对应到 `SQLAlchemy` 支持类型的问题，当前类型支持详见 `《DM8_dmPython使用手册》`  5.3节类型映射

* 修复了自增列自增值设置报错问题

* 修复了列类型为 `Enum` 时访问该列时报错的问题


#### dmSQLAlchemy v2.0.1(2024.08.27)

* 修复了单条语句执行时长最大为30秒的问题，现执行语句默认将不再限制执行时长
* 新增了对于SQLAlchemy的 `array` 类型的支持

#### dmSQLAlchemy v2.0.0(2023.01.06)

* 修复了主键为自增列的情况下执行插入操作报错的问题