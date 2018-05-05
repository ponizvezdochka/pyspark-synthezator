import re
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from synthezator import Synthezator


class Generator():

    def __init__(self, sqlContext=None, seed=None):
        self.sqlContext = sqlContext if sqlContext else\
            SparkSession \
            .builder \
            .master('local') \
            .getOrCreate()
        self.synthezator = Synthezator(seed)

    def create_table(self, table_nm, columns, formats, n_rows):
        table_df = self.synthezator.generate_table(columns, formats, n_rows)

        fields = []
        for col_name, col_type in zip(columns, formats):
            try:
                field = StructField(col_name, StringType(), True)
                if col_type == 'bigint':
                    field = StructField(col_name, LongType(), True)
                elif col_type in ['int', 'integer', 'smallint', 'tinyint']:
                    field = StructField(col_name, IntegerType(), True)
                elif 'decimal' in col_type or 'numeric' in col_type:
                    format_ = re.sub(' |[a-z()]', '', col_type)
                    if ',' in format_:
                        precision = format_.split(',')[0]
                        scale = format_.split(',')[1]
                    else:
                        precision = format_
                        scale = format_
                    field = StructField(col_name, DecimalType(precision=int(precision), scale=int(scale)), True)
                elif col_type == 'float':
                    field = StructField(col_name, FloatType(), True)
                elif col_type == 'double':
                    field = StructField(col_name, DoubleType(), True)
                elif col_type == 'timestamp':
                    field = StructField(col_name, TimestampType(), True)
                elif col_type == 'date':
                    field = StructField(col_name, DateType(), True)
                elif col_type == 'binary':
                    field = StructField(col_name, BinaryType(), True)
                elif col_type == 'boolean':
                    field = StructField(col_name, BooleanType(), True)
                fields.append(field)
            except:
                print('Unknown datatype: %s' % col_type)
                return

        synthetic_df = self.sqlContext.createDataFrame(table_df, schema=StructType(fields))
        synthetic_df.registerTempTable(table_nm)