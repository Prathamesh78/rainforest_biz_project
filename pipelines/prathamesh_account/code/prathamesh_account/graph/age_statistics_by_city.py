from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from prathamesh_account.config.ConfigStore import *
from prathamesh_account.functions import *

def age_statistics_by_city(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("City"))

    return df1.agg(max(col("Age")).alias("Max_Age"), min(col("Age")).alias("Min_Age"), avg(col("Age")).alias("Avg_Age"))
