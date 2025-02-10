from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from prathamesh_account.config.ConfigStore import *
from prathamesh_account.functions import *

def Orders(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Order_ID", IntegerType(), True), StructField("Customer_ID", IntegerType(), True), StructField("Product", StringType(), True), StructField("Quantity", IntegerType(), True), StructField("Price", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/Orders.csv")
