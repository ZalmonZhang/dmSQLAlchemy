from __future__ import absolute_import

from sqlalchemy import types as sqltypes
from sqlalchemy.sql import operators

idx_precedence = operators._PRECEDENCE[operators.json_getitem_op]

ASTEXT = operators.custom_op(
    "$.",
    precedence=idx_precedence,
    natural_self_precedent=True,
    eager_grouping=True,
)

JSONPATH_ASTEXT = operators.custom_op(
    "#>>",
    precedence=idx_precedence,
    natural_self_precedent=True,
    eager_grouping=True,
)

class JSON(sqltypes.JSON):
    __visit_name__ = 'JSON'

    def get_dbapi_type(self, dbapi):
        return dbapi.VARCHAR

    # 不再覆盖 bind_processor（对齐 SQLAlchemy 主流方言：PG/MySQL/SQLite/MSSQL 均不覆盖）：
    # 交回基类 sqltypes.JSON 处理，由 dialect._json_serializer（见 dmpython.py 的
    # my_json_serializer = json.dumps）对所有非 None 值真序列化；None 走 core 语义
    # （none_as_null=False）写成 JSON 'null'，而非 SQL NULL。
    # 历史实现只对 dict 调用 json.dumps、其余值原样透传，导致 list/标量未被序列化：
    # [] 被写成 SQL NULL（静默丢数据）、'hello' 触发 -3105、'' 落 SQL NULL、True 写成 '1'。
    def result_processor(self, dialect, coltype):

        @dialect.compatible_module.json_proc_decorator
        def process(value):
            return value.upper()

        return process

    class Comparator(sqltypes.JSON.Comparator):
        """Define comparison operations for :class:`.JSON`."""

        @property
        def astext(self):
            if isinstance(self.expr.right.type, sqltypes.JSON.JSONPathType):
                return self.expr.left.operate(
                    JSONPATH_ASTEXT,
                    self.expr.right,
                    result_type=self.type.astext_type,
                )
            else:
                return self.expr.left.operate(
                    ASTEXT, self.expr.right, result_type=self.type.astext_type
                )

    comparator_factory = Comparator


class _FormatTypeMixin(object):
    def _format_value(self, value):
        raise NotImplementedError()

    def bind_processor(self, dialect):
        super_proc = self.string_bind_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process

    def literal_processor(self, dialect):
        super_proc = self.string_literal_processor(dialect)

        def process(value):
            value = self._format_value(value)
            if super_proc:
                value = super_proc(value)
            return value

        return process


class JSONIndexType(_FormatTypeMixin, sqltypes.JSON.JSONIndexType):
    def _format_value(self, value):
        value = '$.%s' % value
        return value


class JSONPathType(_FormatTypeMixin, sqltypes.JSON.JSONPathType):
    def _format_value(self, value):
        return '$%s' % (
            "".join(
                [
                     '.%s' % elem
                    for elem in value
                ]
            )
        )
