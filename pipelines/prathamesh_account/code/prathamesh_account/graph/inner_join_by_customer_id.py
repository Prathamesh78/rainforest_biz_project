from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from prathamesh_account.config.ConfigStore import *
from prathamesh_account.functions import *

def inner_join_by_customer_id(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.Customer_ID") == col("in1.Customer_ID")), "inner")
