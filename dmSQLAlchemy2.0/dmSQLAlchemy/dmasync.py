from __future__ import annotations

import re
import json
import dmPython
import functools
import collections
from . import dmpython as _dmPython
from sqlalchemy import sql, text, exc, pool, util
from sqlalchemy.engine import default, reflection
from typing import Any, Union, Callable, Optional, Type
from sqlalchemy.util import await_only, asbool, await_fallback
from .extensions import OracleCompatible_Mode, MySQLCompatible_Mode, TSQLCompatible_Mode, DMMySQLDialect_Adapter, Quote_Method
from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection, AsyncAdapt_dbapi_cursor, AsyncAdapt_dbapi_ss_cursor, AsyncAdaptFallback_dbapi_connection

from . import __name__ as MODULE_NAME

from .extensions import globalvars

Xid = collections.namedtuple(
    "Xid", ["format_id", "global_transaction_id", "branch_qualifier"]
)

def params_initer(f):
    def wrapped_f(self, *args, **kwargs):
        f(self, *args, **kwargs)
        self._impl = self._impl_class()
        if kwargs:
            self._impl.set(kwargs)

    return wrapped_f


class ConnectParams:
    __module__ = MODULE_NAME
    __slots__ = ["_impl"]

    @params_initer
    def __init__(
        self,
        *,
        user=None,
        password=None,
        host=None,
        port=None,
    ):
        pass

class Connection:
    def __init__(self, host="localhost", user=None, password="", txn_isolation=None,
                 port=5236, conn_class=None, access_mode=None, compress_msg=None,
                 autoCommit=None, connection_timeout=None, login_timeout=None,
                 use_stmt_pool=None, ssl_path=None, mpp_login=None, rwseparate=None,
                 rwseparate_percent=None, lang_id=None, local_code=None):

        self.host = host
        self.port = port
        self.user = user or 'SYSDBA'
        self.password = password or ""
        self.txn_isolation = txn_isolation
        self.conn_class = conn_class
        self.access_mode = access_mode
        self.compress_msg = compress_msg
        self.autoCommit = autoCommit
        self.connection_timeout = connection_timeout
        self.login_timeout = login_timeout
        self.use_stmt_pool = use_stmt_pool
        self.ssl_path = ssl_path
        self.mpp_login = mpp_login
        self.rwseparate = rwseparate
        self.rwseparate_percent = rwseparate_percent
        self.lang_id = lang_id
        self.local_code = local_code

    async def _connect(self, cargs):
        import dmAsync
        cargs['connection_timeout'] = cargs['connection_timeout'] if cargs['connection_timeout'] else 0
        # 如果连接串中存在schema，则采用连接串中的schema，SQLAlchemy中为database，忽略后来设置的schema=''，否则采用schema=''，均无则采取默认
        if 'database' in cargs:
            schema = cargs['database']
            cargs['schema'] = schema
            del cargs['database']

        conn = dmAsync.connect(**cargs)
        self.conn = conn
        return self

class dmConnection(dmPython.Connection):

    def __init__(self, connection):
        self.version = connection.server_version

class AsyncConnection(dmPython.Connection):
    __module__ = MODULE_NAME

    def __init__(
        self,
        dsn,
        params,
        kwargs,
    ):
        self._connect_coroutine = self._connect(params, kwargs)

    def __await__(self):
        coroutine = self._connect_coroutine
        self._connect_coroutine = None
        return coroutine.__await__()

    async def __aenter__(self):
        if self._connect_coroutine is not None:
            await self._connect_coroutine
        else:
            self._verify_connected()
        return self

    async def __aexit__(self, *exc_info):
        if self._impl is not None:
            await self._impl.close()
            self._impl = None

    async def _connect(self, params, kwargs):
        if params is None:
            conn = Connection(*kwargs)
            await conn._connect(kwargs)
        else:
            conn = Connection(*params, **kwargs)
            await conn._connect(*params, **kwargs)
        self._impl=conn.conn
        return self._impl

    def _verify_can_execute(
        self, parameters, keyword_parameters
    ):
        self._verify_connected()
        if keyword_parameters:
            if parameters:
                raise
            return keyword_parameters
        elif parameters is not None and not isinstance(
            parameters, (list, tuple, dict)
        ):
            raise
        return parameters

    async def callfunc(
        self,
        name,
        return_type,
        parameters=None,
        keyword_parameters=None,
    ):
        with self.cursor() as cursor:
            return await cursor.callfunc(
                name, return_type, parameters, keyword_parameters
            )

    async def callproc(
        self,
        name,
        parameters=None,
        keyword_parameters=None,
    ):
        with self.cursor() as cursor:
            return await cursor.callproc(name, parameters, keyword_parameters)

    async def changepassword(
        self, old_password, new_password
    ):
        self._verify_connected()
        await self._impl.change_password(old_password, new_password)

    async def close(self):
        self._verify_connected()
        await self._impl.close()
        self._impl = None

    async def commit(self):
        self._verify_connected()
        await self._impl.commit()

    def cursor(self, scrollable=False):
        self._verify_connected()
        return AsyncCursor(self, scrollable)

    async def execute(
        self,
        statement,
        parameters=None,
    ):
        with self.cursor() as cursor:
            await cursor.execute(statement, parameters)

    async def executemany(
        self, statement, parameters
    ):
        with self.cursor() as cursor:
            await cursor.executemany(statement, parameters)

    async def fetchall(
        self,
        statement,
        parameters=None,
        arraysize=None,
        rowfactory=None,
    ):
        with self.cursor() as cursor:
            if arraysize is not None:
                cursor.arraysize = arraysize
            cursor.prefetchrows = cursor.arraysize
            await cursor.execute(statement, parameters)
            cursor.rowfactory = rowfactory
            return await cursor.fetchall()

    async def fetch_df_all(
        self,
        statement,
        parameters=None,
        arraysize=None,
    ):
        cursor = self.cursor()
        cursor._impl.fetching_arrow = True
        if arraysize is not None:
            cursor.arraysize = arraysize
        cursor.prefetchrows = cursor.arraysize
        await cursor.execute(statement, parameters)
        return await cursor._impl.fetch_df_all(cursor)

    async def fetch_df_batches(
        self,
        statement,
        parameters=None,
        size=None,
    ):
        cursor = self.cursor()
        cursor._impl.fetching_arrow = True
        if size is not None:
            cursor.arraysize = size
        cursor.prefetchrows = cursor.arraysize
        await cursor.execute(statement, parameters)
        if size is None:
            yield await cursor._impl.fetch_df_all(cursor)
        else:
            async for df in cursor._impl.fetch_df_batches(cursor, size):
                yield df

    async def fetchmany(
        self,
        statement,
        parameters=None,
        num_rows=None,
        rowfactory=None,
    ):
        with self.cursor() as cursor:
            if num_rows is None:
                num_rows = cursor.arraysize
            elif num_rows <= 0:
                return []
            cursor.arraysize = cursor.prefetchrows = num_rows
            await cursor.execute(statement, parameters)
            cursor.rowfactory = rowfactory
            return await cursor.fetchmany(num_rows)

    async def fetchone(
        self,
        statement,
        parameters=None,
        rowfactory=None,
    ):
        with self.cursor() as cursor:
            cursor.prefetchrows = cursor.arraysize = 1
            await cursor.execute(statement, parameters)
            cursor.rowfactory = rowfactory
            return await cursor.fetchone()

    async def ping(self):
        self._verify_connected()
        await self._impl.ping()

    async def rollback(self):
        self._verify_connected()
        await self._impl.rollback()

    async def tpc_begin(
        self, xid, flags=0x00000001, timeout=0
    ):
        self._verify_connected()
        self._verify_xid(xid)
        await self._impl.tpc_begin(xid, flags, timeout)

    async def tpc_commit(
        self, xid=None, one_phase=False
    ):
        self._verify_connected()
        if xid is not None:
            self._verify_xid(xid)
        await self._impl.tpc_commit(xid, one_phase)

    async def tpc_end(
        self, xid=None, flags=0
    ):
        self._verify_connected()
        if xid is not None:
            self._verify_xid(xid)
        if flags not in (0, 0x00100000):
            raise
        await self._impl.tpc_end(xid, flags)

    async def tpc_forget(self, xid):
        self._verify_connected()
        self._verify_xid(xid)
        await self._impl.tpc_forget(xid)

    async def tpc_prepare(self, xid=None):
        self._verify_connected()
        if xid is not None:
            self._verify_xid(xid)
        return await self._impl.tpc_prepare(xid)

    async def tpc_recover(self):
        with self.cursor() as cursor:
            await cursor.execute(
                """
                    select
                        formatid,
                        globalid,
                        branchid
                    from dba_pending_transactions"""
            )
            cursor.rowfactory = Xid
            return await cursor.fetchall()

    async def tpc_rollback(self, xid=None):
        self._verify_connected()
        if xid is not None:
            self._verify_xid(xid)
        await self._impl.tpc_rollback(xid)

def _async_connection_factory(
    f
):
    @functools.wraps(f)
    def connect_async(
        dsn=None,
        *,
        conn_class=AsyncConnection,
        params=None,
        **kwargs,
    ):
        f(
            dsn=dsn,
            conn_class=conn_class,
            params=params,
            **kwargs,
        )
        if not issubclass(conn_class, AsyncConnection):
            raise

        if params is not None and not isinstance(params, ConnectParams):
            raise

        return conn_class(dsn, params, kwargs)

    return connect_async


@_async_connection_factory
def connect_async(
    dsn=None,
    *,
    user=None,
    password=None,
    host=None,
    server=None,
    port=None,
    conn_class=AsyncConnection,
    params=None,
    access_mode=None,
    autoCommit=None,
    connection_timeout=None,
    login_timeout=None,
    txn_isolation=None,
    compress_msg=None,
    use_stmt_pool=None,
    ssl_path=None,
    ssl_pwd=None,
    mpp_login=None,
    ukey_name=None,
    ukey_pin=None,
    rwseparate=None,
    rwseparate_percent=None,
    lang_id=None,
    local_code=None,
    cursorclass=None,
    database=None,
    schema=None,
    shake_crypto=None,
    dmsvc_path=None,
    parse_type=None,
    _timeout=None,
):
    pass

class BaseCursor:
    _impl = None

    def __init__(
        self,
        connection,
        scrollable=False,
    ):
        self.connection = connection
        self._impl = connection._impl.create_cursor_impl(scrollable)

    def __del__(self):
        if self._impl is not None:
            self._impl.close(in_del=True)

    def __enter__(self):
        self._verify_open()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self._verify_open()
        self._impl.close(in_del=True)
        self._impl = None

    def __repr__(self):
        typ = self.__class__
        cls_name = f"{typ.__module__}.{typ.__qualname__}"
        return f"<{cls_name} on {self.connection!r}>"

    def _call(
        self,
        name,
        parameters,
        keyword_parameters,
        return_value=None,
    ):
        if parameters is not None and not isinstance(parameters, (list, tuple)):
            raise
        if keyword_parameters is not None and not isinstance(
                keyword_parameters, dict
        ):
            raise
        self._verify_open()
        statement, bind_values = self._call_get_execute_args(name, parameters, keyword_parameters, return_value)
        return self.execute(statement, bind_values)

    def _call_get_execute_args(
        self,
        name,
        parameters,
        keyword_parameters,
        return_value=None,
    ):
        bind_names = []
        bind_values = []
        statement_parts = ["begin "]
        if return_value is not None:
            statement_parts.append(":retval := ")
            bind_values.append(return_value)
        statement_parts.append(name + "(")
        if parameters:
            bind_values.extend(parameters)
            bind_names = [":%d" % (i + 1) for i in range(len(parameters))]
        if keyword_parameters:
            for arg_name, arg_value in keyword_parameters.items():
                bind_values.append(arg_value)
                bind_names.append(f"{arg_name} => :{len(bind_names) + 1}")
        statement_parts.append(",".join(bind_names))
        statement_parts.append("); end;")
        statement = "".join(statement_parts)
        return (statement, bind_values)

    def _prepare(
        self, statement, tag=None, cache_statement=True
    ):
        self._impl.prepare(statement, tag, cache_statement)

    def _prepare_for_execute(
        self, statement, parameters, keyword_parameters=None
    ):
        self._verify_open()
        self._impl._prepare_for_execute(
            self, statement, parameters, keyword_parameters
        )

    def _verify_fetch(self):
        self._verify_open()
        if not self._impl.is_query(self):
            raise

    def _verify_open(self):
        if self._impl is None:
            raise
        self.connection._verify_connected()

    @property
    def arraysize(self):
        self._verify_open()
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, value):
        self._verify_open()
        if not isinstance(value, int) or value <= 0:
            raise
        self._impl.arraysize = value

    def arrayvar(
        self,
        typ,
        value,
        size=0,
    ):
        self._verify_open()
        if isinstance(value, list):
            num_elements = len(value)
        elif isinstance(value, int):
            num_elements = value
        else:
            raise TypeError("expecting integer or list of values")
        var = self._impl.create_var(
            self.connection,
            typ,
            size=size,
            num_elements=num_elements,
            is_array=True,
        )
        if isinstance(value, list):
            var.setvalue(0, value)
        return var

    def bindnames(self):
        self._verify_open()
        if self._impl.statement is None:
            raise
        return self._impl.get_bind_names()

    @property
    def bindvars(self):
        self._verify_open()
        return self._impl.get_bind_vars()

    def close(self):
        self._verify_open()
        self._impl.close()
        self._impl = None

    @property
    def fetchvars(self):
        self._verify_open()
        return self._impl.get_fetch_vars()

    def getarraydmlrowcounts(self):
        self._verify_open()
        return self._impl.get_array_dml_row_counts()

    def getbatcherrors(self):
        self._verify_open()
        return self._impl.get_batch_errors()

    def getimplicitresults(self):
        self._verify_open()
        return self._impl.get_implicit_results(self.connection)

    @property
    def inputtypehandler(self):
        self._verify_open()
        return self._impl.inputtypehandler

    @inputtypehandler.setter
    def inputtypehandler(self, value):
        self._verify_open()
        self._impl.inputtypehandler = value

    @property
    def lastrowid(self):
        self._verify_open()
        return self._impl.get_lastrowid()

    @property
    def outputtypehandler(self):
        self._verify_open()
        return self._impl.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value):
        self._verify_open()
        self._impl.outputtypehandler = value

    @property
    def prefetchrows(self):
        self._verify_open()
        return self._impl.prefetchrows

    @prefetchrows.setter
    def prefetchrows(self, value):
        self._verify_open()
        self._impl.prefetchrows = value

    def prepare(
        self, statement, tag=None, cache_statement=True
    ):
        self._verify_open()
        self._prepare(statement, tag, cache_statement)

    @property
    def rowcount(self):
        if self._impl is not None and self.connection._impl is not None:
            return self._impl.rowcount
        return -1

    @property
    def rowfactory(self):
        self._verify_open()
        return self._impl.rowfactory

    @rowfactory.setter
    def rowfactory(self, value):
        self._verify_open()
        self._impl.rowfactory = value

    @property
    def scrollable(self):
        self._verify_open()
        return self._impl.scrollable

    @scrollable.setter
    def scrollable(self, value):
        self._verify_open()
        self._impl.scrollable = value

    def setinputsizes(self, *args, **kwargs):
        if args and kwargs:
            raise
        elif args or kwargs:
            self._verify_open()
            return self._impl.setinputsizes(self.connection, args, kwargs)
        return []

    def setoutputsize(self, size, column=0):
        pass

    @property
    def statement(self):
        if self._impl is not None:
            return self._impl.statement

    def var(
        self,
        typ,
        size=0,
        arraysize=1,
        inconverter=None,
        outconverter=None,
        typename=None,
        encoding_errors=None,
        bypass_decode=False,
        convert_nulls=False,
        *,
        encodingErrors=None,
    ):
        self._verify_open()
        if typename is not None:
            typ = self.connection.gettype(typename)
        elif typ is dmPython.objedctvar:
            raise
        if encodingErrors is not None:
            if encoding_errors is not None:
                raise
            encoding_errors = encodingErrors
        return self._impl.create_var(
            self.connection,
            typ,
            size,
            arraysize,
            inconverter,
            outconverter,
            encoding_errors,
            bypass_decode,
            convert_nulls=convert_nulls,
        )

    @property
    def warning(self):
        self._verify_open()
        return self._impl.warning


class Cursor(BaseCursor):
    __module__ = MODULE_NAME

    def __iter__(self):
        return self

    def __next__(self):
        self._verify_fetch()
        row = self._impl.fetch_next_row(self)
        if row is not None:
            return row
        raise StopIteration

    def _get_oci_attr(self, attr_num, attr_type):
        self._verify_open()
        return self._impl._get_oci_attr(attr_num, attr_type)

    def _set_oci_attr(self, attr_num, attr_type, value):
        self._verify_open()
        self._impl._set_oci_attr(attr_num, attr_type, value)

    def callfunc(
        self,
        name,
        return_type,
        parameters=None,
        keyword_parameters=None,
        *,
        keywordParameters=None,
    ):
        var = self.var(return_type)
        if keywordParameters is not None:
            if keyword_parameters is not None:
                raise
            keyword_parameters = keywordParameters
        self._call(name, parameters, keyword_parameters, var)
        return var.getvalue()

    def callproc(
        self,
        name,
        parameters=None,
        keyword_parameters=None,
        *,
        keywordParameters=None,
    ):
        if keywordParameters is not None:
            if keyword_parameters is not None:
                raise
            keyword_parameters = keywordParameters
        self._call(name, parameters, keyword_parameters)
        if parameters is None:
            return []
        return [
            v.get_value(0) for v in self._impl.bind_vars[: len(parameters)]
        ]

    def execute(
        self,
        statement,
        parameters=None,
        **keyword_parameters,
    ):
        self._prepare_for_execute(statement, parameters, keyword_parameters)
        impl = self._impl
        impl.execute(self)
        if impl.fetch_vars is not None:
            return self

    def executemany(
        self,
        statement,
        parameters,
        batcherrors=False,
        arraydmlrowcounts=False,
    ):
        self._verify_open()
        num_execs = self._impl._prepare_for_executemany(
            self, statement, parameters
        )
        yield self._impl.executemany(
            self, num_execs, bool(batcherrors), bool(arraydmlrowcounts)
        )

    def fetchall(self):
        self._verify_fetch()
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while True:
            row = fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    def fetchmany(
        self, size=None, numRows=None
    ):
        self._verify_fetch()
        if size is None:
            if numRows is not None:
                size = numRows
            else:
                size = self._impl.arraysize
        elif numRows is not None:
            raise
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while len(result) < size:
            row = fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    def fetchone(self):
        self._verify_fetch()
        return self._impl.fetch_next_row(self)

    def parse(self, statement):
        self._verify_open()
        self._prepare(statement)
        self._impl.parse(self)

    def scroll(self, value=0, mode="relative"):
        self._verify_open()
        self._impl.scroll(self, value, mode)


class AsyncCursor(BaseCursor):
    __module__ = MODULE_NAME

    async def __aenter__(self):
        self._verify_open()
        return self

    async def __aexit__(self, *exc_info):
        self._verify_open()
        self._impl.close(in_del=True)
        self._impl = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._verify_fetch()
        row = await self._impl.fetch_next_row(self)
        if row is not None:
            return row
        raise StopAsyncIteration

    async def callfunc(
        self,
        name,
        return_type,
        parameters=None,
        keyword_parameters=None,
    ):
        var = self.var(return_type)
        await self._call(name, parameters, keyword_parameters, var)
        return var.getvalue()

    async def callproc(
        self,
        name,
        parameters=None,
        keyword_parameters=None,
    ):
        await self._call(name, parameters, keyword_parameters)
        if parameters is None:
            return []
        return [
            v.get_value(0) for v in self._impl.bind_vars[: len(parameters)]
        ]

    async def execute(
        self,
        statement,
        parameters=None,
        **keyword_parameters,
    ):
        self._prepare_for_execute(statement, parameters, keyword_parameters)
        yield await self._impl.execute(self)

    async def executemany(
        self,
        statement,
        parameters,
        batcherrors=False,
        arraydmlrowcounts=False,
    ):
        self._verify_open()
        num_execs = self._impl._prepare_for_executemany(
            self, statement, parameters
        )
        yield await self._impl.executemany(
            self, num_execs, bool(batcherrors), bool(arraydmlrowcounts)
        )

    async def fetchall(self):
        self._verify_fetch()
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while True:
            row = await fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    async def fetchmany(self, size=None):
        self._verify_fetch()
        if size is None:
            size = self._impl.arraysize
        result = []
        fetch_next_row = self._impl.fetch_next_row
        while len(result) < size:
            row = await fetch_next_row(self)
            if row is None:
                break
            result.append(row)
        return result

    async def fetchone(self):
        self._verify_fetch()
        return await self._impl.fetch_next_row(self)

    async def parse(self, statement):
        self._verify_open()
        self._prepare(statement)
        await self._impl.parse(self)

    async def scroll(self, value=0, mode="relative"):
        self._verify_open()
        await self._impl.scroll(self, value, mode)

class DMExecutionContext_dmasync(
    _dmPython.DMExecutionContext_dmPython
):
    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()

        if self.dialect.arraysize:
            cursor.arraysize = self.dialect.arraysize

        return cursor

    def set_output_val(self, value):
        self.cursor._cursor.raw.output_stream = value

class DMDialect_dmAsync(_dmPython.DMDialect_dmPython):
    supports_statement_cache = True
    execution_ctx_cls = DMExecutionContext_dmasync
    is_async = True
    driver = "dmasync"
    _min_version = (1,)

    def __init__(
        self,
        auto_convert_lobs=True,
        coerce_to_decimal=True,
        arraysize=None,
        encoding_errors=None,
        thick_mode=None,
        **kwargs,
    ):
        self.async_connect = None
        super().__init__(
            auto_convert_lobs,
            coerce_to_decimal,
            arraysize,
            encoding_errors,
            **kwargs,
        )

        self.async_connect = self.async_connect

    @classmethod
    def import_dbapi(cls):
        import dmPython

        return DMAdaptDBAPI(dmPython)

    def connect(self, *cargs, **cparams):
        try:
            compatible_mode = None
            if 'compatible_mode' in cparams:
                if type(cparams['compatible_mode']) is str and cparams['compatible_mode'].upper() in ['DM', 'MYSQL', 'TSQL', 'ORACLE']:
                    compatible_mode = cparams['compatible_mode'].upper()
                    del cparams['compatible_mode']
                    if compatible_mode == 'MYSQL':
                        self.compatible_module = MySQLCompatible_Mode()
                    elif compatible_mode == 'TSQL':
                        self.compatible_module = TSQLCompatible_Mode()
                    elif compatible_mode == 'ORACLE':
                        self.compatible_module = OracleCompatible_Mode()
                else:
                    raise ValueError("The compatible_mode must be of string type and specified within the scope of DM, Oracle, MYSQL and TSQL")

            if 'parse_type' in cparams:
                parse_type = cparams['parse_type']
                if type(parse_type) is str and parse_type.upper() in ['DM', 'MYSQL', 'TSQL']:
                    if parse_type.upper() == 'MYSQL':
                        if compatible_mode is None:
                            self.compatible_module = MySQLCompatible_Mode()
                        self.parse_module = DMMySQLDialect_Adapter()
                        self.parse_stmt_func = _dmPython.parse_mysql_stmt
                    if parse_type.upper() == 'TSQL':
                        if compatible_mode is None:
                            self.compatible_module = TSQLCompatible_Mode()
                        self.parse_stmt_func = _dmPython.parse_tsql_stmt
                else:
                    raise ValueError("The parse_type must be of string type and specified within the scope of DM, MYSQL and TSQL")

            if 'cursorclass' in cparams:
                if cparams['cursorclass'] != 0:
                    raise ValueError("In dmSQLAlchemy, the cursorclass option is only allowed to be dmPython.TupleCursor")

            if 'add_quote_all' in cparams:
                quote_type = cparams['add_quote_all']
                if type(quote_type) is bool:
                    if quote_type is True:
                        self.quote_module = Quote_Method()
                    del cparams['add_quote_all']
                else:
                    raise ValueError("The add_quote_all must be of bool type")

            if 'database' in cparams:
                schema = cparams['database']
                cparams['schema'] = schema
                del cparams['database']

            dbapi_conn = self.dbapi.connect(*cargs, **cparams)

            conn = dbapi_conn

            conn_raw = conn.driver_connection.raw

            self.encoding = self.get_conn_local_code(conn_raw)

            self.case_sensitive = conn_raw.str_case_sensitive

            self.async_connect = dbapi_conn.driver_connection

            if self.case_sensitive:
                self.requires_name_normalize = True
            else:
                self.requires_name_normalize = False

            cursor = conn_raw.cursor()
            cursor.execute('SELECT MODE$ FROM V$instance;')
            result = cursor.fetchall()
            if result is not None:
                mode_str = result[0][0]
                if (mode_str != 'STANDBY'):
                    cursor.execute('SELECT SET_SESSION_IDENTITY_CHECK(1);')
            else:
                cursor.execute('SELECT SET_SESSION_IDENTITY_CHECK(1);')
            return conn
        except self.dbapi.DatabaseError as err:
            raise

    @classmethod
    def is_thin_mode(cls, connection):
        return connection.connection.dbapi_connection.thin

    @classmethod
    def get_async_dialect_cls(cls, url):
        return DMDialectAsync_dmasync

    def _load_version(self, dbapi_module):
        version = (0, 0, 0)
        if dbapi_module is not None:
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", dbapi_module.version)
            if m:
                version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )
        self.dmasync_ver = version
        if self.dmasync_ver > (0, 0, 0) and self.dmasync_ver < self._min_version:
            raise exc.InvalidRequestError(
                f"dmasync version {self._min_version} and above are supported"
            )

    @classmethod
    def get_pool_class(cls, url):
        async_fallback = url.query.get("async_fallback", False)

        if util.asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def _get_default_schema_name(self, connection):
        self.trace_process('DMDialectAsync_dmasync', '_get_default_schema_name', connection)
        dbapi_conn = connection.connection.dbapi_connection.driver_connection.raw

        if hasattr(dbapi_conn, 'current_schema') and dbapi_conn.current_schema is not None:
            return self.normalize_name(dbapi_conn.current_schema)
        else:
            return self.normalize_name(connection.execute(sql.text('SELECT USER FROM DUAL')).scalar())

    def _get_server_version_info(self, connection):
        self.dbapi_conn = connection.connection.dbapi_connection
        if self.async_connect is None:
            self.async_connect = self.dbapi_conn
        dbapi_conn = connection.connection.dbapi_connection
        version = []
        r = re.compile(r'[.\-]')
        for n in r.split(dbapi_conn.version):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    def do_begin_twophase(self, connection, xid):
        conn_xis = connection.connection.xid(*xid)
        connection.connection.tpc_begin(conn_xis)
        connection.connection.info["dmasync_xid"] = conn_xis

    def do_prepare_twophase(self, connection, xid):
        should_commit = connection.connection.tpc_prepare()
        connection.info["dmasync_should_commit"] = should_commit

    def do_rollback_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        if recover:
            conn_xid = connection.connection.xid(*xid)
        else:
            conn_xid = None
        connection.connection.tpc_rollback(conn_xid)

    def do_commit_twophase(
        self, connection, xid, is_prepared=True, recover=False
    ):
        conn_xid = None
        if not is_prepared:
            should_commit = connection.connection.tpc_prepare()
        elif recover:
            conn_xid = connection.connection.xid(*xid)
            should_commit = True
        else:
            should_commit = connection.info["dmasync_should_commit"]
        if should_commit:
            connection.connection.tpc_commit(conn_xid)

    def do_recover_twophase(self, connection):
        return [
            (
                fi,
                gti.decode() if isinstance(gti, bytes) else gti,
                bq.decode() if isinstance(bq, bytes) else bq,
            )
            for fi, gti, bq in connection.connection.tpc_recover()
        ]

    def do_close(self, dbapi_connection):
        dbapi_connection.close()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def get_isolation_level(self, dbapi_connection):
        try:
            cursor = await_only(self.async_connect.cursor())
            await_only(cursor.execute(
                "SELECT CASE ISOLATION WHEN 1 THEN 'READ COMMITTED' WHEN 0 THEN 'READ UNCOMMITTED' ELSE 'SERIALIZABLE' END AS isolation_level"
                " FROM V$TRX WHERE ID = dbms_transaction.local_transaction_id( TRUE );",
            ))
            row = await_only(cursor.fetchone())
            if row is None:
                raise exc.InvalidRequestError(
                    "could not retrieve isolation level"
                )
            result = row[0]

            return result
        except:
            raise

    @reflection.cache
    def has_table(self, connection, table_name, schema = None, dblink = None, **kw):

        name = self.denormalize_name(table_name),
        schema_name = self.denormalize_name(schema or self.default_schema_name)
        statement = """SELECT tables_and_views.table_name 
                    FROM (SELECT a_tables.table_name AS table_name, a_tables.owner AS owner 
                    FROM all_tables a_tables UNION ALL SELECT a_views.view_name AS table_name, a_views.owner AS owner 
                    FROM all_views a_views) tables_and_views 
                    WHERE tables_and_views.table_name = ? AND tables_and_views.owner = ?"""
        cursor = self.dm_do_execute(connection.connection.connection.cursor(), statement, [name[0], schema_name])
        result = cursor.fetchone() is not None
        return result

    def set_isolation_level(self, dbapi_connection, level):
        try:
            if level == "AUTOCOMMIT":
                dbapi_connection.autocommit = True
            else:
                dbapi_connection.autocommit = False
                dbapi_connection.rollback()
                cursor = await_only(self.async_connect.cursor())
                await_only(cursor.execute(f"SET SESSION CHARACTERISTICS AS ISOLATION LEVEL {level}"))
        except:
            raise

    def _check_max_identifier_length(self, connection):
        max_len = connection.connection.connection.max_identifier_length
        if max_len is not None:
            return max_len
        else:
            return super()._check_max_identifier_length(connection)
        
    # For internal use only, with known parameters and no need for returning
    def dm_do_execute(self, cursor, statement, parameters, context=None):
        await_only(cursor.execute(statement, parameters))
        if context != None:
            context.cursor = cursor
        return cursor

    def do_execute(self, cursor, statement, parameters, context=None):
        try:
            if parameters != [] and parameters != None:
                for i in range(len(parameters)):
                    list_element = parameters[i]
                    if type(list_element) is list:
                        if len(list_element) == 0:
                            result_string = ''
                        else:
                            result_string = json.dumps(list_element)
                        list_element = result_string
                        parameters[i] = list_element
            version_info = globalvars.get_var('DMPYTHON_VERSION').split(".")
            if int(version_info[0]) > 2 or (int(version_info[0]) == 2 and int(version_info[1]) > 5) or (
                    int(version_info[0]) == 2 and int(version_info[1]) == 5 and int(version_info[2]) > 9):
                if context != None:
                    if context.out_parameters != None:
                        if context.compiled is not None:
                            if hasattr(context.compiled, '_dm_returning'):
                                if context.compiled._dm_returning:
                                    poslist, parameters = self.resort_output_params(parameters, context)
                                    result = context.cursor._cursor.raw.execute(statement, parameters)
                                    for i in range(len(context.out_parameters)):
                                        if result[poslist[i]] == [None]:
                                            context.out_parameters[f"ret_{i}"] = []
                                        else:
                                            context.out_parameters[f"ret_{i}"] = result[poslist[i]]
                                    return
            await_only(cursor.execute(statement, parameters))
            if context != None:
                context.cursor = cursor
            return cursor
        except Exception as error:
            raise

    def do_executemany(self, cursor, statement, parameters, context=None):
        try:
            if isinstance(parameters, tuple):
                parameters = list(parameters)
            import datetime
            rows = len(parameters)
            columns = len(parameters[0]) if parameters else 0
            for i in range(rows):
                for j in range(columns):
                    if type(parameters[i][j]) is datetime.datetime:
                        temp = parameters[i][j]
                        str_temp = temp.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
                        if 'UTC' in str_temp:
                            parameters[i][j] = str_temp.replace('UTC', '')
                        else:
                            parameters[i][j] = str_temp
                    if type(parameters[i][j]) is list:
                        list_element = parameters[i][j]
                        if len(list_element) == 0:
                            result_string = ''
                        else:
                            result_string = json.dumps(list_element)
                        list_element = result_string
                        parameters[i][j] = list_element
            self.parse_module.async_do_executemany_return(columns, rows, self, cursor, statement, parameters, context)

            if context is not None:
                context.cursor = cursor
        except Exception as error:
            raise


class AsyncAdapt_dmasync_cursor(AsyncAdapt_dbapi_cursor):

    _cursor = None
    __slots__ = ()

    @property
    def outputtypehandler(self):
        return self._cursor.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value):
        self._cursor.outputtypehandler = value

    def var(self, *args, **kwargs):
        return self._cursor.raw.var(*args, **kwargs)

    def close(self):
        self._rows.clear()
        self._cursor.close()

    def setinputsizes(self, *args, **kwargs):
        return self._cursor.setinputsizes(*args, **kwargs)

    def _make_new_cursor(
        self, connection
    ):
        return await_only(self._connection.cursor())

    async def _execute_async(self, operation, parameters):

        if parameters is None:
            result = self._cursor.execute(operation)
        else:
            result = self._cursor.execute(operation, parameters)

        return result

    async def _async_soft_close(self):
        return

    async def _executemany_async(
        self,
        operation,
        seq_of_parameters,
    ):
        return self._cursor.executemany(operation, seq_of_parameters)

    def fetchall(self):
        result = self._rows
        if self._cursor.description and not self.server_side:
            result = self._cursor.raw.fetchall()
        retval = list(result)
        self._rows.clear()
        return retval

    def fetchone(self):
        result = self._rows
        if self._cursor.description and not self.server_side:
            result = self._cursor.raw.fetchone()
        if result:
            return result
        else:
            return None

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

class AsyncAdapt_dmasync_ss_cursor(
    AsyncAdapt_dbapi_ss_cursor, AsyncAdapt_dmasync_cursor
):
    __slots__ = ()

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None  # type: ignore

class AsyncAdapt_dmasync_connection(AsyncAdapt_dbapi_connection):
    _connection = None
    __slots__ = ()

    thin = True

    _cursor_cls = AsyncAdapt_dmasync_cursor
    _ss_cursor_cls = None

    def __init__(self, dbapi, connection):
        super().__init__(dbapi, connection)
        self.dbapi = dbapi
        self._connection = connection

    @property
    def autocommit(self):
        return self._connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._connection.autocommit = value

    @property
    def outputtypehandler(self):
        return self._connection.outputtypehandler

    @outputtypehandler.setter
    def outputtypehandler(self, value):
        self._connection.outputtypehandler = value

    @property
    def version(self):
        return self._connection.version

    @property
    def stmtcachesize(self):
        return self._connection.stmtcachesize

    @stmtcachesize.setter
    def stmtcachesize(self, value):
        self._connection.stmtcachesize = value

    @property
    def max_identifier_length(self):
        return self._connection.max_identifier_length

    def cursor(self):
        return AsyncAdapt_dmasync_cursor(self)

    def ss_cursor(self):
        return AsyncAdapt_dmasync_ss_cursor(self)

    def xid(self, *args, **kwargs):
        return self._connection.xid(*args, **kwargs)

    def tpc_begin(self, *args, **kwargs):
        return self.await_(self._connection.tpc_begin(*args, **kwargs))

    def tpc_commit(self, *args, **kwargs):
        return self.await_(self._connection.tpc_commit(*args, **kwargs))

    def tpc_prepare(self, *args, **kwargs):
        return self.await_(self._connection.tpc_prepare(*args, **kwargs))

    def tpc_recover(self, *args, **kwargs):
        return self.await_(self._connection.tpc_recover(*args, **kwargs))

    def tpc_rollback(self, *args, **kwargs):
        return self.await_(self._connection.tpc_rollback(*args, **kwargs))

class AsyncAdaptFallback_dmasync_connection(
    AsyncAdaptFallback_dbapi_connection, AsyncAdapt_dmasync_connection
):
    __slots__ = ()

class DMAdaptDBAPI:
    def __init__(self, dmPython):
        self.dmPython = dmPython

        for k, v in self.dmPython.__dict__.items():
            if k != "connect":
                self.__dict__[k] = v

    def connect(self, *arg, **kw):
        async_fallback = kw.pop("async_fallback", False)
        creator_fn = kw.pop("async_creator_fn", connect_async)

        if asbool(async_fallback):
            return AsyncAdaptFallback_dmasync_connection(
                self, await_fallback(creator_fn(*arg, **kw))
            )

        else:
            return AsyncAdapt_dmasync_connection(
                self, await_only(creator_fn(*arg, **kw))
            )

class DMExecutionContextAsync_dmasync(DMExecutionContext_dmasync):
    create_cursor = default.DefaultExecutionContext.create_cursor

    def create_default_cursor(self):
        cursor = self._dbapi_connection.raw.cursor()

        if self.dialect.arraysize:
            cursor.arraysize = self.dialect.arraysize

        return cursor

    def create_server_side_cursor(self):
        c = self._dbapi_connection.raw.cursor()
        if self.dialect.arraysize:
            c.arraysize = self.dialect.arraysize

        return c

    def create_cursor(self):
        cursor = self._dbapi_connection.cursor()

        if self.dialect.arraysize:
            cursor.arraysize = self.dialect.arraysize

        return cursor

    def get_cols_from_lastrowid(self, table, primary_columns, lastrowid):
        reserved_words = set([x.lower() for x in globalvars.get_var('RESERVED_WORDS')])

        table_name = self._self_process_name(table.name, reserved_words)
        statement = "SELECT "
        for i in range(len(primary_columns)):
            if i > 0:
                statement = statement + ', '
            primary_columns_name = self._self_process_name(primary_columns[i].name, reserved_words)
            statement = statement + primary_columns_name
        statement = statement + " from {} where rowid = ?".format(table_name)

        self.dialect.dm_do_execute(self.cursor, statement, [lastrowid], None)
        return self.cursor.fetchone()

class DMDialectAsync_dmasync(DMDialect_dmAsync):
    supports_server_side_cursors = True
    supports_statement_cache = True
    execution_ctx_cls = DMExecutionContextAsync_dmasync

    _min_version = (2,)

    @classmethod
    def import_dbapi(cls):
        import dmPython

        return DMAdaptDBAPI(dmPython)

    @classmethod
    def get_pool_class(cls, url):
        async_fallback = url.query.get("async_fallback", False)

        if asbool(async_fallback):
            return pool.FallbackAsyncAdaptedQueuePool
        else:
            return pool.AsyncAdaptedQueuePool

    def get_driver_connection(self, connection):
        return connection._connection

dialect = DMDialect_dmAsync
dialect_async = DMDialectAsync_dmasync
