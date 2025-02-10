from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from prathamesh_account.config.ConfigStore import *
from prathamesh_account.functions import *

def test_01(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Customer_ID", IntegerType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True), StructField("Age", IntegerType(), True), StructField("City", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Customers.csv")
