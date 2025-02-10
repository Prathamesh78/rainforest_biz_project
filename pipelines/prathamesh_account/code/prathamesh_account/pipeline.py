from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prathamesh_account.config.ConfigStore import *
from prathamesh_account.functions import *
from prophecy.utils import *
from prathamesh_account.graph import *

def pipeline(spark: SparkSession) -> None:
    df_test_01 = test_01(spark)
    df_Orders = Orders(spark)
    df_age_statistics_by_city = age_statistics_by_city(spark, df_test_01)
    df_inner_join_by_customer_id = inner_join_by_customer_id(spark, df_test_01, df_Orders)

def main():
    spark = SparkSession.builder.enableHiveSupport().appName("prathamesh_account").getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/prathamesh_account")
    spark.conf.set("spark.default.parallelism", "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/prathamesh_account", config = Config)(pipeline)

if __name__ == "__main__":
    main()
