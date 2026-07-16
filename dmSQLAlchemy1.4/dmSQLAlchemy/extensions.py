import json
from sqlalchemy import util, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.elements import quoted_name

RESERVED_WORDS = \
    set('IFNULL ABSOLUTE ADD ALL ALTER AND ANY ARRAYLEN AS ASC ASSIGN AUDIT BEGIN BETWEEN '
        'BIGDATEDIFF BOOL BOTH BSTRING BY BYTE CALL CASE CAST TREAT CHAR CHECK CLUSTER FOR '
        'COLUMN COMMIT COMMITWORK COMMENT CONNECT CONNECT_BY_ROOT CONSTRAINT CONTAINS GOTO '
        'CONTEXT CONVERT CORRESPONDING CREATE CRYPTO CURRENT CURSOR DATEADD DATEDIFF IN IF '
        'DATEPART DECIMAL DECLARE DECODE DEFAULT DELETE DESC DISTINCT DISTRIBUTED DOUBLE IS '
        'DROP ELSE ELSEIF END EXECUTE EXISTS EXIT EXPLAIN EXTRACT FETCH FINAL FIRST FLOAT '
        'FOREIGN FROM FULLY FUNCTION GRANT GROUP HAVING IDENTITY IMMEDIATE INDEX INSERT INT '
        'INTERVAL INTO  LEAD LIKE LOGIN LOOP MEMBER NEW NEXT NOT NULL OBJECT OF ON OR ORDER '
        'OUT PARTITION PENDANT PERCENT PRIMARY PRINT PRIOR PRIVILEGES PROCEDURE PUBLIC RAISE '
        'RECORD REF REFERENCES REFERENCE REFERENCING RELATIVE REPEAT RETURN REVERSE REVOKE '
        'ROLLBACK ROW ROWNUM ROWS SAVEPOINT SCHEMA SELECT SET SOME SUBPARTITION SWITCH SYNONYM '
        'TABLE TIMESTAMPADD TIMESTAMPDIFF TO TOP TRAIL TRIGGER TRIM TRUNCATE UNION UNIQUE UNTIL '
        'UPDATE USER USING VALUES VARRAY VIEW WHEN WHENEVER WHILE WITH DISKSPACE RETURNING '
        'SBYTE SHORT USHORT UINT ULONG VOID CONST DO BREAK CONTINUE THROW FINALLY TRY CATCH '
        'PROTECTED PRIVATE ABSTRACT SEALED STATIC VIRTUAL OVERRIDE EXTERN CLASS STRUCT GET '
        'SIZEOF TYPEOF ADMIN REPLICATE VERIFY EQU EXCHANGE CLUSTERBTR LIST ARRAY ROLLUP CUBE '
        'GROUPING OVER SECTION SETS DOMAIN COLLATION OVERLAY EVERY KEEP WITHIN LNNVL NOCOPY '
        'INLINE TYPEDEF XMLTABLE XMLNAMESPACES XMLPARSE XMLAGG AUTO_INCREMENT BINARY XMLELEMENT '
        'XMLATTRIBUTES XMLSERIALIZE XMLQUERY LEXER FLASHBACK NOCYCLE NOSORT OPTIMIZE VERSIONS '
        'LARGE WITHOUT PIPE XML JSON_TABLE LESS THAN MODEL DIMENSION XMLCAST SQL_CALC_FOUND_ROWS'
        'YEAR PCTFREE UID MODE WHERE DATETIME DATE LOCK SHARE NOCOMPRESS VALUE LIMIT NOWAIT RAW '
        'VARCHAR LEVEL EXCLUSIVE THEN INTEGER IDENTIFIED SMALLINT LONG START RENAME VARCHAR2 '
        'MINUS INTERSECT OPTION SIZE RESOURCE NUMBER COMPRESS'.split())

class GlobalVars:
    _shared_data = {}

    @classmethod
    def set_var(cls, key, value):
        cls._shared_data[key] = value

    @classmethod
    def get_var(cls, key, default=None):
        return cls._shared_data.get(key, default)

globalvars = GlobalVars()
globalvars.set_var('RESERVED_WORDS', RESERVED_WORDS)

class dmSession(Session):
    def execute(
            self,
            statement,
            params=None,
            execution_options=util.EMPTY_DICT,
            bind_arguments=None,
            _parent_execute_state=None,
            _add_event=None,
            **kw
    ):

        if self.bind.dialect.parse_stmt_func is not None and not isinstance(statement, TextClause):
            if not isinstance(self, Session) and not isinstance(self, Connection):
                raise ValueError("The db_session must be an instance object of Session or Connection of SQLAlchemy")

            raw_sql, params = self.bind.dialect.parse_stmt_func(statement)

            result = self.execute(text(raw_sql), params,  execution_options=execution_options, bind_arguments=bind_arguments, _parent_execute_state=_parent_execute_state, _add_event=_add_event)
            return result
        else:
            return super(dmSession, self).execute(
                statement,
                params,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
                _parent_execute_state=_parent_execute_state,
                _add_event=_add_event,
                **kw
            )

class dmsessionmaker(sessionmaker):

    def __init__(
        self,
        bind=None,
        class_=dmSession,
        autoflush=True,
        autocommit=False,
        expire_on_commit=True,
        info=None,
        **kw
    ):
        super(dmsessionmaker, self).__init__(bind, class_=class_, autoflush=autoflush, autocommit=autocommit, expire_on_commit=expire_on_commit, info=info, **kw)

class DMDialect_Adapter:

    autoincrement_str = " IDENTITY(1, 1)"
    initial_quote = final_quote = '"'
    escape_quote = '"'
    escape_to_quote = escape_quote * 2

    def limit_offset_clause(self, dialect, select, fetch_clause, fetch_clause_options, **kw):

        text = ''

        if select._offset_clause is not None:
            offset_str = dialect.process(select._offset_clause, **kw)
            text += "\n OFFSET %s ROWS" % offset_str

        if fetch_clause is not None:
            text += "\n FETCH FIRST %s%s ROWS %s" % (
                dialect.process(fetch_clause, **kw),
                " PERCENT" if fetch_clause_options["percent"] else "",
                "WITH TIES" if fetch_clause_options["with_ties"] else "ONLY",
            )

        return text

class DMMySQLDialect_Adapter:

    autoincrement_str = " AUTO_INCREMENT"
    initial_quote = final_quote = '`'
    escape_quote = '`'
    escape_to_quote = '``'

    def limit_offset_clause(self, dialect, select, fetch_clause, fetch_clause_options, **kw):

        text = ''

        if fetch_clause is not None:
            text += "\nLIMIT %s" % (
                dialect.process(fetch_clause, **kw)
            )

        if select._offset_clause is not None:
            if fetch_clause is None:
                text += "\nLIMIT 9223372036854775807"
            offset_str = dialect.process(select._offset_clause, **kw)
            text += "\nOFFSET %s" % offset_str

        return text

class NoCompatible_Mode:

    def json_proc_decorator(self, func):
        def process(value):
            if type(value) is dict or type(value) is list:
                return str(value)
            return value

        return process

class MySQLCompatible_Mode(NoCompatible_Mode):

    def json_proc_decorator(self, func):
        def process(value):
            if type(value) is dict:
                return value
            else:
                return json.loads(value)

        return process

class TSQLCompatible_Mode(NoCompatible_Mode):

    pass

class OracleCompatible_Mode(NoCompatible_Mode):

    pass


class Quote_Method:

    def normalize_name(self, dialect, name):
        return quoted_name(name, quote=True)

    def denormalize_name(self, dialect, name):
        if util.py2k:
            name = unicode(name)
        return name

    def quote_ident(self, identifier_prepare, ident):
        return identifier_prepare.quote_identifier(ident)

    def return_quote_str(self, identifier_prepare, ident):
        return identifier_prepare.quote_identifier(ident)

    def _need_quote(self, result_name, escape_quote, reserved_words):
        return True

class No_Quote_Method:

    def normalize_name(self, dialect, name):
        if name.upper() == name and not \
                dialect.identifier_preparer._requires_quotes(name.lower()):
            return name.lower()
        elif name.lower() == name:
            return quoted_name(name, quote=True)
        else:
            return name

    def denormalize_name(self, dialect, name):
        if name.lower() == name and not \
                dialect.identifier_preparer._requires_quotes(name.lower()):
            name = name.upper()
        if util.py2k:
            name = unicode(name)
        return name

    def quote_ident(self, identifier_prepare, ident):
        if identifier_prepare._requires_quotes(ident):
            return identifier_prepare.quote_identifier(ident)
        else:
            return ident

    def return_quote_str(self, identifier_prepare, ident):
        return ident

    def _need_quote(self, result_name, escape_quote, reserved_words):
        if result_name.lower() in reserved_words or escape_quote in result_name or (
                result_name.upper() != result_name and result_name.lower() != result_name):
            return True
        else:
            return False

