import os
import ast
import enum
import sqlalchemy
import uuid
import contextlib
import logging
import decimal
from itertools import zip_longest
from dataclasses import dataclass
from sqlalchemy import types, text, literal_column, String, Column, Text, JSON, DateTime, create_engine, inspect
from sqlalchemy.orm import declarative_base, Session

MAX_DIM = 65535
MIN_DIM = 1

logger = logging.getLogger()

class BreakLoop(Exception):
    pass

def _encode_vector(value, dim=None, storage_format=None):
    import numpy
    if value is None:
        return value

    if storage_format is not None:
        if storage_format.upper() == 'SPARSE' and len(value) != 2 and len(value) != 3:
            raise ValueError(f"The list length of input data of sparse vector type is only allowed to be 2 or 3")
        if storage_format.upper() == 'DENSE' and dim is not None and dim != '*' and len(value) != dim:
            raise ValueError(f"expected {dim} dimensions, but got {len(value)}")

    if isinstance(value, numpy.ndarray):
        if value.ndim != 1:
            raise ValueError("expected ndim to be 1")
        return f"[{','.join(map(str, value))}]"

    return str(value)

def _decode_vector(value):
    if value is None:
        return value

    return ast.literal_eval(value)

def is_1d(lst):
    return all(isinstance(item, (int, float)) for item in lst)

class DistanceMetric(enum.Enum):
    DOT = "DOT"
    COSINE = "COSINE"
    HAMMING = "HAMMING"
    EUCLIDEAN = "EUCLIDEAN"
    MANHATTAN = "MANHATTAN"
    EUCLIDEAN_SQUARED = "EUCLIDEAN_SQUARED"

    def to_sql_func(self):
        if self in DistanceMetric:
            return self.value
        else:
            raise ValueError("Unsupported distance metric")

class VECTORTYPE(types.UserDefinedType):
    __visit_name__ = 'VECTOR'

class VECTOR(VECTORTYPE):
    cache_ok = True
    dim = 0

    def __init__(self, dim=None, format=None, storage_format=None):
        if dim is None:
            raise ValueError(f"Unsupported by DM")

        if dim != '*':
            if not isinstance(dim, int):
                raise ValueError("Dimension must be of type integer or None")

            if dim < MIN_DIM or dim > MAX_DIM:
                raise ValueError(f"The range of dimension values is from {MIN_DIM} to {MAX_DIM}")

        if format is None:
            format = 'FLOAT32'
        else:
            format = format.upper()
        if format not in ['INT8', 'FLOAT32', 'FLOAT64', 'BINARY', '*']:
            raise ValueError("Unsupported Type by DM, format must be within the range of INT8, FLOAT32, FLOAT64, BINARY, *")

        if storage_format is None:
            storage_format = 'DENSE'

        if type(storage_format) is not str or storage_format.upper() not in ['SPARSE', 'DENSE']:
            raise ValueError(f"Currently, the storage_format only supports being set to 'DENSE' or 'SPARSE'")
        else:
            storage_format = storage_format.upper()

        super(types.UserDefinedType, self).__init__()
        self.dim = dim
        self.format = format
        self.storage_format = storage_format

    def get_col_spec(self, **kw):
        if self.dim is None:
            return "VECTOR"
        return f"VECTOR({self.dim}, {self.format}, '{self.storage_format}')"

    def bind_processor(self, dialect):

        def process(value):
            return _encode_vector(value, self.dim, self.storage_format)

        return process

    def result_processor(self, dialect, coltype):

        def process(value):
            return _decode_vector(value)

        return process

    class comparator_factory(types.UserDefinedType.Comparator):

        def l1_distance(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.L1_DISTANCE(self, literal_column(with_sign_str)).label(
                "L1_DISTANCE"
            )

        def l2_distance(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.L2_DISTANCE(self, literal_column(with_sign_str)).label(
                "L2_DISTANCE"
            )

        def l2_s_distance(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.VECTOR_DISTANCE(self, literal_column(with_sign_str), literal_column('EUCLIDEAN_SQUARED')).label(
                "VECTOR_DISTANCE"
            )

        def cosine_distance(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.COSINE_DISTANCE(self, literal_column(with_sign_str)).label(
                "COSINE_DISTANCE"
            )

        def inner_product(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.INNER_PRODUCT(self, literal_column(with_sign_str)).label(
                "INNER_PRODUCT"
            )

        def hamming_distance(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.HAMMING_DISTANCE(self, literal_column(with_sign_str)).label(
                "HAMMING_DISTANCE"
            )

        def inner_product_negative(self, other):
            formatted_other = _encode_vector(other)
            with_sign_str = "TO_VECTOR('" + formatted_other + "', " + str(self.type.dim) + ", " + self.type.format
            if is_1d(other):
                with_sign_str += ", DENSE)"
            else:
                with_sign_str += ", SPARSE)"
            return sqlalchemy.func.INNER_PRODUCT_NEGATIVE(self, literal_column(with_sign_str)).label(
                "INNER_PRODUCT_NEGATIVE"
            )

class VectorAdaptor:

    engine = None

    def __init__(self, engine):
        self.engine = engine
        self.conn = self.engine.connect()

    def close(self):
        self.conn.close()

    @staticmethod
    def _check_vector_column(column):
        if not isinstance(column.type, VECTOR):
            raise ValueError("Not a vector column")

    @staticmethod
    def _has_vector_index(conn, owner, table_name, column_name):
        query = text(f"SELECT ALL_IND_COL.column_name, ALL_IND_COL.table_name, USER_IND.index_type FROM ALL_IND_COLUMNS"
                     f" ALL_IND_COL, USER_INDEXES USER_IND WHERE ALL_IND_COL.INDEX_OWNER = USER_IND.TABLE_OWNER AND"
                     f" ALL_IND_COL.INDEX_NAME = USER_IND.INDEX_NAME AND ALL_IND_COL.TABLE_OWNER = :owner"
                     f" AND ALL_IND_COL.TABLE_NAME = :table_name AND ALL_IND_COL.COLUMN_NAME = :column_name;"
                     ).bindparams(owner=owner, table_name=table_name, column_name=column_name)
        result = conn.execute(query)
        result_dict = result.mappings().all()
        for row in result_dict:
            if (conn.dialect.denormalize_name(row["column_name"]) == column_name and
                    conn.dialect.denormalize_name(row['table_name']) == table_name):
                return True, row["index_type"]
        return False, None

    def create_index(
            self,
            column,
            index_type,
            metric_name='COSINE',
            percentage_value=90,
            num_of_partitions=None,
            max_connection=None,
            ef_construction=None,
            scope=None,
            block=None,
            index_name=None,
            owner=None,
            skip_existing=False
    ):
        if index_type is None or type(index_type) is not str or index_type.upper() not in ["IVF", "HNSW", "BMP"]:
            raise ValueError(
                "The index_type must be specified and fall within the range of 'IVF', 'HNSW' and 'BMP'"
            )

        self._check_vector_column(column)
        if column.type.dim is None:
            raise ValueError(
                "Vector index is only supported for fixed dimension vectors"
            )

        conn = self.conn
        owner = conn.dialect.denormalize_name(owner or conn.dialect.default_schema_name)
        table_name = conn.dialect.denormalize_name(column.table.name)
        column_name = conn.dialect.denormalize_name(column.name)

        has_flag, exist_type = self._has_vector_index(conn, owner, table_name, column_name)

        if skip_existing and has_flag:
            print(f"The current column already has a vector index with index type {exist_type},"
                  f" so creation has been skipped.")
            return

        table_name = conn.dialect.identifier_preparer.quote_identifier(table_name)
        column_name = conn.dialect.identifier_preparer.quote_identifier(column_name)
        index_type = index_type.upper()
        if index_type == "IVF":
            index_name = conn.dialect.identifier_preparer.quote_identifier(conn.dialect.denormalize_name(index_name or "ivf_ind_" + column.table.name))
            self._create_vector_ivf_index(conn, table_name, column_name, index_name, metric_name, percentage_value,
                                          num_of_partitions)
        elif index_type == "HNSW":
            index_name = conn.dialect.identifier_preparer.quote_identifier(conn.dialect.denormalize_name(index_name or "hnsw_ind_" + column.table.name))
            self._create_vector_hnsw_index(conn, table_name, column_name, index_name, metric_name, percentage_value,
                                          max_connection, ef_construction)
        elif index_type == "BMP":
            index_name = conn.dialect.identifier_preparer.quote_identifier(conn.dialect.denormalize_name(index_name or "bmp_ind_" + column.table.name))
            self._create_vector_bmp_index(conn, table_name, column_name, index_name, scope, metric_name, block)

    @staticmethod
    def _create_vector_ivf_index(
        conn,
        table_name,
        column_name,
        index_name,
        metric_name="COSINE",
        percentage_value=90,
        num_of_partitions=0,
    ):
        query_str = f"CREATE VECTOR INDEX %(index_name)s on %(table_name)s(%(column_name)s) ORGANIZATION PARTITIONS\n"\
            "DISTANCE %(metric_name)s WITH TARGET ACCURACY %(percentage_value)s"

        if num_of_partitions is not None:
            query_str += " PARAMETERS(TYPE IVF, NEIGHBOR PARTITIONS " + str(num_of_partitions) + ");"
        metric_name = DistanceMetric(metric_name.upper())

        query_text = query_str % {'index_name': index_name, 'table_name': table_name,
                                  'column_name': column_name, 'metric_name': metric_name.to_sql_func(),
                                  'percentage_value': percentage_value}
        conn.execute(text(query_text))

    @staticmethod
    def _create_vector_hnsw_index(
        conn,
        table_name,
        column_name,
        index_name,
        metric_name="COSINE",
        percentage_value=90,
        max_connection=0,
        ef_construction=0,
    ):
        query_str = f"CREATE VECTOR INDEX %(index_name)s on %(table_name)s(%(column_name)s) ORGANIZATION GRAPH\n"\
            "DISTANCE %(metric_name)s WITH TARGET ACCURACY %(percentage_value)s"

        if max_connection is not None or ef_construction is not None:
            if max_connection is not None:
                query_str += " PARAMETERS(TYPE HNSW, NEIGHBOR " + str(max_connection)
                if ef_construction is not None:
                    query_str += ", EFCONSTRUCTION " + str(ef_construction) + ");"
                else:
                    query_str += ");"
            else:
                query_str += " PARAMETERS(TYPE HNSW, EFCONSTRUCTION " + str(ef_construction) + ");"
        metric_name = DistanceMetric(metric_name.upper())
        query_text = query_str % {'index_name': index_name, 'table_name': table_name, 'column_name': column_name,
                                  'metric_name': metric_name.to_sql_func(), 'percentage_value': percentage_value}
        conn.execute(text(query_text))

    @staticmethod
    def _create_vector_bmp_index(
            conn,
            table_name,
            column_name,
            index_name,
            scope=None,
            metric_name="COSINE",
            block=0
    ):
        query_str = f"CREATE VECTOR INDEX %(index_name)s ON %(table_name)s(%(column_name)s) "
        if scope is not None:
            query_str += scope
        query_str += " ORGANIZATION NEIGHBOR PARTITIONS BITMAP DISTANCE %(metric_name)s "
        metric_name = DistanceMetric(metric_name.upper())
        query_text = query_str % {'index_name': index_name, 'table_name': table_name, 'column_name': column_name,
                                  'metric_name': metric_name.to_sql_func()}
        if block is not None:
            query_text += "PARAMETERS(TYPE BMP, block " + str(block) + ");"
            conn.execute(text(query_text))
        else:
            conn.execute(text(query_text))

    @staticmethod
    def _check_index_match(conn, schema_name, table_name, column_name, index_name):
        query_str = (f"SELECT ALL_IND_COL.index_name, USER_IND.index_type FROM ALL_IND_COLUMNS"
                     f" ALL_IND_COL, USER_INDEXES USER_IND WHERE ALL_IND_COL.INDEX_OWNER = USER_IND.TABLE_OWNER AND"
                     f" ALL_IND_COL.INDEX_NAME = USER_IND.INDEX_NAME AND ALL_IND_COL.TABLE_OWNER = :owner"
                     f" AND ALL_IND_COL.TABLE_NAME = :table_name AND ALL_IND_COL.COLUMN_NAME = :column_name;")
        result = conn.execute(
            text(query_str).bindparams(owner=schema_name, table_name=table_name, column_name=column_name))
        if result.rowcount == 0:
            raise ValueError("There is no index on this vector column")

        for row_dict in result:
            if index_name is not None and index_name != conn.dialect.denormalize_name(row_dict[0]):
                raise ValueError("Incorrect index name or column information input")
            else:
                return conn.dialect.denormalize_name(row_dict[0]), row_dict[1]

    def rebuild_index(
            self,
            column=None,
            schema_name=None,
            index_name=None,
            index_type=None,
            metric_name=None,
            block=None,
            target_accuracy=None,
            cluster_centers=None,
            max_connection=None,
            ef_construction=None,
    ):
        if column is None and index_name is None:
            raise ValueError(
                "At least column information or index name is required"
            )

        if column is not None:
            self._check_vector_column(column)

        conn = self.conn
        schema_name = conn.dialect.denormalize_name(schema_name or conn.dialect.default_schema_name)
        index_name = conn.dialect.denormalize_name(index_name) if index_name is not None else None

        if column is not None:
            column_name = conn.dialect.denormalize_name(column.name)
            table_name_an = conn.dialect.denormalize_name(column.table.name)
            index_name, ind_type = self._check_index_match(conn, schema_name, table_name_an, column_name, index_name)
        else:
            ind_type, table_name = self._get_index_info(conn, schema_name, index_name)

        if ind_type == "VECTOR HNSW":
            ind_type = "HNSW"
        elif ind_type == "VECTOR IVFFLAT":
            ind_type = "IVF"
        elif ind_type == "VECTOR BMP":
            ind_type = "BMP"
        else:
            raise ValueError(
                "Index type mismatch, only supports rebuilding HNSW index, IVF index and BMP index"
            )

        if index_type is not None and ind_type != index_type.upper():
            raise ValueError(
                f"The index type found does not match the specified index type. The found type is {ind_type} while"
                f" the specified type is {index_type}"
            )

        if ind_type == "HNSW":
            self._rebuild_vector_hnsw_index(conn, schema_name, index_name, metric_name, target_accuracy, max_connection,
                                            ef_construction)
        elif ind_type == "IVF":
            self._rebuild_vector_ivf_index(conn, schema_name, index_name, metric_name, target_accuracy, cluster_centers)
        elif ind_type == "BMP":
            self._rebuild_vector_bmp_index(conn, schema_name, index_name, metric_name, block)

    def _get_index_info(self, conn, schema_name, index_name):
        query_str = ("SELECT INDEX_TYPE, TABLE_NAME FROM USER_INDEXES WHERE TABLE_OWNER = :schema_name "
                     "AND INDEX_NAME = :index_name")
        result = conn.execute(
            text(query_str).bindparams(schema_name=schema_name, index_name=index_name))
        if result.rowcount != 1:
            raise ValueError(f"Error occurred while obtaining the vector index {index_name} in  {schema_name} schema")
        else:
            return result.fetchone()

    @staticmethod
    def _rebuild_vector_ivf_index(
            conn=None,
            schema_name=None,
            index_name=None,
            metric_name=None,
            target_accuracy=None,
            cluster_centers=None,
    ):

        query_str = ("CALL SP_REBUILD_VECTOR_IVFFLAT_INDEX(:schema_name, :index_name, "
                     ":metric_name, :target_accuracy, :cluster_centers);")

        conn.execute(text(query_str).bindparams(schema_name=schema_name, index_name=index_name, metric_name=metric_name,
                                                target_accuracy=target_accuracy, cluster_centers=cluster_centers))
        return

    @staticmethod
    def _rebuild_vector_hnsw_index(
            conn=None,
            schema_name=None,
            index_name=None,
            metric_name=None,
            target_accuracy=0,
            max_connection=0,
            ef_construction=0
    ):

        query_str = ("CALL SP_REBUILD_VECTOR_HNSW_INDEX(:schema_name, :index_name, "
                     ":metric_name, :target_accuracy, :max_connection, :ef_construction);")

        conn.execute(text(query_str).bindparams(schema_name=schema_name, index_name=index_name, metric_name=metric_name,
                                                target_accuracy=target_accuracy, max_connection=max_connection,
                                                ef_construction=ef_construction))
        return

    @staticmethod
    def _rebuild_vector_bmp_index(
            conn=None,
            schema_name=None,
            index_name=None,
            metric_name=None,
            block=None,
    ):

        query_str = "CALL SP_REBUILD_VECTOR_BMP_INDEX(:schema_name, :index_name, :metric_name, "

        if block is None:
            query_str += "NULL);"
        else:
            query_str += f"{block});"

        conn.execute(text(query_str).bindparams(schema_name=schema_name, index_name=index_name, metric_name=metric_name))
        return

@dataclass
class QueryResult:
    def __init__(self, id, document, metadata, distance):
        self.id = id
        self.document = document
        self.metadata = metadata
        self.distance = distance

def _create_vector_table_model(
    table_name,
    dim=None,
):

    BaseOrm = declarative_base()

    class VectorTableModel(BaseOrm):

        __tablename__ = table_name
        id = Column(
            String(36), primary_key=True, default=lambda: str(uuid.uuid4())
        )
        embedding = Column(
            VECTOR(dim),
            nullable=False,
        )
        document = Column(Text, nullable=True)
        meta = Column(JSON, nullable=True)
        create_time = Column(
            DateTime, server_default=text("CURRENT_TIMESTAMP")
        )
        update_time = Column(
            DateTime,
            server_default=text(
                "CURRENT_TIMESTAMP"
            ),
        )

    return BaseOrm, VectorTableModel

class VectorWordSeek:
    def __init__(
            self,
            connection_str=None,
            table_name=None,
            vector_dim=None,
            drop_if_existing=False,
            model=None,
            model_path=None,
            engine_args=None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self._conn_str = connection_str
        self._table_name = table_name
        self._vector_dim = vector_dim
        self._drop_if_existing = drop_if_existing
        self._engine_args = engine_args if engine_args else {}
        self._engine = self._create_engine()
        self._model = model
        self._model_path = model_path
        self._vector_col_name = 'embedding'
        self._check_model_dim()
        self._check_table_compatibility()

        self._orm_base, self._table_model = _create_vector_table_model(
            self._table_name, self._vector_dim
        )

    def _create_engine(self):
        return create_engine(url=self._conn_str, **self._engine_args)

    def _check_table_compatibility(self):
        if self._drop_if_existing:
            return

        conn = self._engine.connect()
        schema_name = conn.dialect.denormalize_name(conn.dialect.default_schema_name)
        table_name = conn.dialect.denormalize_name(self._table_name)

        if(conn.dialect.has_table(conn, table_name, schema_name) is False):
            return

        inspector = inspect(self._engine)
        columns = inspector.get_columns(table_name)

        if columns is None or len(columns) != 6:
            raise ValueError(
                "The existing table named " + table_name + " does not match the table to be created"
            )

        try:
            for row_dict in columns:
                if row_dict['name'] not in ['id', 'embedding', 'document', 'meta', 'create_time', 'update_time']:
                    raise BreakLoop
                if row_dict['name'] == 'id':
                    if row_dict['type'].python_type != str or row_dict['type'].length < decimal.Decimal('36'):
                        raise BreakLoop
                if row_dict['name'] == 'embedding':
                    if (type(row_dict['type']) is not VECTOR or row_dict['type'].dim != self._vector_dim or
                            row_dict['type'].format != 'FLOAT32'):
                        raise BreakLoop
        except BreakLoop:
            raise ValueError(
                "The existing table named " + table_name + " does not match the table to be created"
            )

    def _check_model_dim(self):
        if self._model is not None:
            dimension = self._model.get_sentence_embedding_dimension()
        elif self._model_path is not None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_path)
            dimension = self._model.get_sentence_embedding_dimension()
        else:
            raise ValueError("No model has been loaded, model needs to be provided")

        if self._vector_dim is None:
            self._vector_dim = dimension

        if self._vector_dim != dimension:
            raise ValueError(
                "The dimension of model does not match the dimension of the table"
            )

    def create_table(self, drop_if_existing):
        if drop_if_existing:
            self.drop_table()
        with Session(self._engine) as session, session.begin():
            self._orm_base.metadata.create_all(session.get_bind())

    def drop_table(self):
        with Session(self._engine) as session, session.begin():
            self._orm_base.metadata.drop_all(session.get_bind())

    @contextlib.contextmanager
    def _make_session(self):
        yield Session(self._engine)

    def insert(
        self,
        texts,
        metadatas=None,
        ids=None,
        **kwargs,
    ):
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        else:
            model = self._model
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]
        if not metadatas:
            metadatas = [{} for _ in texts]

        embeddings = [model.encode(document).flatten().tolist() for document in texts]

        with Session(self._engine) as session:
            for document, metadata, embedding, id_val in list(zip_longest(texts, metadatas, embeddings, ids)):
                embeded_doc = self._table_model(
                    id=id_val,
                    embedding=embedding,
                    document=document,
                    meta=metadata,
                )
                session.add(embeded_doc)
            session.commit()

        return ids

    def delete(
        self,
        ids=None,
        filter=None,
        **kwargs,
    ):
        if ids is None and filter is None:
            raise ValueError(
                f"At least one of the filter parameter and the ids parameter needs to be non-None."
            )
        filter_by = self._build_filter_clause(filter)
        with Session(self._engine) as session:
            if ids is not None:
                filter_by = sqlalchemy.and_(self._table_model.id.in_(ids), filter_by)
            stmt = sqlalchemy.delete(self._table_model).filter(filter_by)
            session.execute(stmt)
            session.commit()

    def query(
        self,
        DistanceMetric,
        query_vector,
        count=5,
        filter=None,
        **kwargs,
    ):
        relevant_docs = self._vector_search(DistanceMetric, query_vector, count, filter, **kwargs)

        return [
            QueryResult(
                document=doc.document,
                metadata=doc.meta,
                id=doc.id,
                distance=doc.distance,
            )
            for doc in relevant_docs
        ]

    def get_distance_func(self, distance_metric):
        if distance_metric == "DOT":
            return self._table_model.embedding.inner_product_negative
        elif distance_metric == "COSINE":
            return self._table_model.embedding.cosine_distance
        elif distance_metric == "HAMMING":
            return self._table_model.embedding.hamming_distance
        elif distance_metric == "EUCLIDEAN":
            return self._table_model.embedding.l2_distance
        elif distance_metric == "MANHATTAN":
            return self._table_model.embedding.l1_distance
        elif distance_metric == "EUCLIDEAN_SQUARED":
            return self._table_model.embedding.l2_s_distance
        elif distance_metric is None:  # default to cosine
            return self._table_model.embedding.cosine_distance
        else:
            raise ValueError(
                f"Got unexpected value for distance: {distance_metric}. "
            )

    def _change_to_vector(self, query):
        if type(query) is tuple or type(query) is list:
            if len(query) != 1 or type(query[0]) is not str:
                raise ValueError(
                    f"Got unexpected value : {query}. "
                )
            else:
                query = query[0]
        elif type(query) is not str:
            raise ValueError(
                f"Got unexpected value : {query}. "
            )

        if self._model is None:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        else:
            model = self._model

        return model.encode(query).flatten().tolist()

    def _vector_search(
        self,
        distance_metric,
        query_embedding,
        k=5,
        filter=None,
        **kwargs,
    ):

        post_filter_enabled = kwargs.get("post_filter_enabled", False)
        post_filter_multiplier = kwargs.get("post_filter_multiplier", 1)

        embedding_vector = self._change_to_vector(query_embedding)

        with Session(self._engine) as session:
            if post_filter_enabled is False or not filter:
                filter_by = self._build_filter_clause(filter)
                results = (
                    session.query(
                        self._table_model.id,
                        self._table_model.meta,
                        self._table_model.document,
                        self.get_distance_func(distance_metric)(embedding_vector).label("distance"),
                    )
                    .filter(filter_by)
                    .order_by(sqlalchemy.asc("distance"))
                    .limit(k)
                    .all()
                )
            else:
                subquery = (
                    session.query(
                        self._table_model.id,
                        self._table_model.meta,
                        self._table_model.document,
                        self.get_distance_func(distance_metric)(embedding_vector).label("distance"),
                    )
                    .order_by(sqlalchemy.asc("distance"))
                    .limit(post_filter_multiplier * k * 10)
                    .subquery()
                )
                filter_by = self._build_filter_clause(filter, subquery.c)
                results = (
                    session.query(
                        subquery.c.id,
                        subquery.c.meta,
                        subquery.c.document,
                        subquery.c.distance,
                    )
                    .filter(filter_by)
                    .order_by(sqlalchemy.asc(subquery.c.distance))
                    .limit(k)
                    .all()
                )
        return results

    def _build_filter_clause(
        self,
        filters=None,
        table_model=None,
    ):

        if table_model is None:
            table_model = self._table_model

        filter_by = sqlalchemy.true()
        if filters is not None:
            filter_clauses = []

            for key, value in filters.items():
                if key.lower() == "$and":
                    and_clauses = [
                        self._build_filter_clause(condition, table_model)
                        for condition in value
                        if isinstance(condition, dict) and condition is not None
                    ]
                    filter_by_metadata = sqlalchemy.and_(*and_clauses)
                    filter_clauses.append(filter_by_metadata)
                elif key.lower() == "$or":
                    or_clauses = [
                        self._build_filter_clause(condition, table_model)
                        for condition in value
                        if isinstance(condition, dict) and condition is not None
                    ]
                    filter_by_metadata = sqlalchemy.or_(*or_clauses)
                    filter_clauses.append(filter_by_metadata)
                elif key.lower() in [
                    "$in",
                    "$nin",
                    "$gt",
                    "$gte",
                    "$lt",
                    "$lte",
                    "$eq",
                    "$ne",
                ]:
                    raise ValueError(
                        f"Got unexpected filter expression: {filter}. "
                        f"Operator {key} must be followed by a meta key. "
                    )
                elif isinstance(value, dict):
                    filter_by_metadata = self._create_filter_clause(
                        table_model, key, value
                    )

                    if filter_by_metadata is not None:
                        filter_clauses.append(filter_by_metadata)
                else:
                    filter_by_metadata = (
                        sqlalchemy.func.json_value(table_model.meta, f"$.{key}")
                        == value
                    )
                    filter_clauses.append(filter_by_metadata)

            filter_by = sqlalchemy.and_(filter_by, *filter_clauses)
        return filter_by

    def _cast_condition_sql(self, filter_condition, param):
        if type(param) is int:
            filter_condition = sqlalchemy.cast(filter_condition, sqlalchemy.Integer)
        elif type(param) is float:
            filter_condition = sqlalchemy.cast(filter_condition, sqlalchemy.Float)

        return filter_condition

    def _create_filter_clause(self, table_model, key, value):
        IN, NIN, GT, GTE, LT, LTE, EQ, NE = (
            "$in",
            "$nin",
            "$gt",
            "$gte",
            "$lt",
            "$lte",
            "$eq",
            "$ne",
        )

        json_key = sqlalchemy.func.json_value(table_model.meta, f"$.{key}")
        value_case_insensitive = {k.lower(): v for k, v in value.items()}

        if IN in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[IN])
            filter_by_metadata = json_key.in_(value_case_insensitive[IN])
        elif NIN in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[NIN])
            filter_by_metadata = ~json_key.in_(value_case_insensitive[NIN])
        elif GT in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[GT])
            filter_by_metadata = json_key > value_case_insensitive[GT]
        elif GTE in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[GTE])
            filter_by_metadata = json_key >= value_case_insensitive[GTE]
        elif LT in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[LT])
            filter_by_metadata = json_key < value_case_insensitive[LT]
        elif LTE in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[LTE])
            filter_by_metadata = json_key <= value_case_insensitive[LTE]
        elif NE in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[NE])
            filter_by_metadata = json_key != value_case_insensitive[NE]
        elif EQ in map(str.lower, value):
            json_key = self._cast_condition_sql(json_key, value_case_insensitive[EQ])
            filter_by_metadata = json_key == value_case_insensitive[EQ]
        else:
            logger.warning(
                f"Unsupported filter operator: {value}. Consider using "
                "one of $in, $nin, $gt, $gte, $lt, $lte, $eq, $ne, $or, $and."
            )
            filter_by_metadata = None

        return filter_by_metadata

    def execute(self, sql, params=None, autocommit=False):
        try:
            with Session(self._engine) as session, session.begin():
                result = session.execute(sqlalchemy.text(sql), params)
                if autocommit:
                    session.commit()  # Ensure changes are committed for non-SELECT statements.
                if sql.strip().lower().startswith("select"):
                    return {"success": True, "result": result.fetchall(), "error": None}
                else:
                    return {"success": True, "result": result, "error": None}
        except Exception as e:
            # Log the error or handle it as needed
            logger.error(f"SQL execution error: {str(e)}")
            return {"success": False, "result": None, "error": str(e)}

class VectorImageSeek(VectorWordSeek):
    def __init__(
            self,
            connection_str=None,
            table_name=None,
            vector_dim=None,
            drop_if_existing=False,
            model=None,
            model_path=None,
            engine_args=None,
            **kwargs
    ):
        super().__init__(connection_str, table_name, vector_dim, drop_if_existing, model, model_path, engine_args, **kwargs)
        self._set_preprocess()

    def _set_preprocess(self):
        import torch
        from torchvision import transforms

        preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )
        ])

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        resnet = self._model.to(device)
        self._device = device
        self._resnet = resnet
        self._preprocess = preprocess

    def _check_model_dim(self):
        import torch
        test_input = torch.randn(1, 3, 256, 256)
        if self._model is not None:
            output = self._model(test_input).squeeze()
            dimension = output.shape[0]
        elif self._model_path is not None:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            model = torch.jit.load(self._model_path, map_location=device)
            output = model(test_input).squeeze()
            dimension = output.shape[0]
            self._model = model
        else:
            raise ValueError("No model has been loaded, model needs to be provided")

        if self._vector_dim is None:
            self._vector_dim = dimension

        if self._vector_dim != dimension:
            raise ValueError(
                "The dimension of model does not match the dimension of the table"
            )

    def check_and_return_path(self, path):
        if type(path) is not str:
            raise ValueError("The dimension of model does not match the dimension of the table")
        else:
            if os.path.exists(path):
                if os.path.isfile(path) and path.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.pgm', '.pbm')):
                    abs_path = os.path.abspath(path)
                    return [abs_path]
                elif os.path.isdir(path):
                    return [os.path.join(os.path.abspath(path), f)
                                 for f in os.listdir(path)
                                 if f.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp', '.pgm', '.pbm'))]
            else:
                raise ValueError(
                    "Path " + path + " does not exists"
                )

    def analyze_input_path(self, path_query):
        path_list = []
        if type(path_query) is tuple or type(path_query) is list:
            for path in path_query:
                path_list += self.check_and_return_path(path)
        elif type(path_query) is str:
            path_list = self.check_and_return_path(path_query)

        return path_list

    def extract_features(self, img_paths):
        import torch
        from PIL import Image

        features = []
        for path in img_paths:
            img = Image.open(path).convert("RGB")
            img_tensor = self._preprocess(img).unsqueeze(0)

            img_tensor = img_tensor.to(self._device)

            with torch.no_grad():
                feature = self._resnet(img_tensor)

            # 转为NumPy数组并压缩为1D向量
            features.append(feature.cpu().numpy().flatten().tolist())

        return features

    def insert(
        self,
        paths,
        metadatas=None,
        ids=None,
        **kwargs,
    ):

        path_list = self.analyze_input_path(paths)
        embeddings = self.extract_features(path_list)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in path_list]
        if not metadatas:
            metadatas = [{} for _ in path_list]

        with Session(self._engine) as session:
            for document, metadata, embedding, id_val in list(zip_longest(path_list, metadatas, embeddings, ids)):
                embeded_doc = self._table_model(
                    id=id_val,
                    embedding=embedding,
                    document=document,
                    meta=metadata,
                )
                session.add(embeded_doc)
            session.commit()

        return ids

    def _change_to_vector(self, query):
        if type(query) is tuple or type(query) is list:
            if len(query) != 1 or type(query[0]) is not str:
                raise ValueError(
                    f"Got unexpected value : {query}. "
                )
            else:
                query = query[0]
        elif type(query) is not str:
            raise ValueError(
                f"Got unexpected value : {query}. "
            )

        path_list = self.analyze_input_path(query)

        if path_list is None or len(path_list) == 0:
            raise ValueError(
                f"No images of the specified type were retrieved at the specified path: {query}, only png, jpg, jpeg, gif, bmp, pgm, pbm image types are supported. "
            )
        elif len(path_list) > 1:
            raise ValueError(
                f"There are multiple images in specified path : {query}. "
            )
        return self.extract_features(path_list)[0]



