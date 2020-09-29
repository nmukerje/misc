## Spark Transform Step
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark_Transform_Step').getOrCreate()

cdf=spark.read.parquet("s3://<s3_bucket>/sqoop_output/customer")
cdf.createOrReplaceTempView('customer_v')
cdf.printSchema()

csdf=spark.read.parquet("s3://<s3_bucket>/sqoop_output/customer_site")
csdf.createOrReplaceTempView('customer_site')
csdf.printSchema()

odf=spark.read.csv("s3://<s3_bucket>/sqoop_output/sales_order").toDF("ORDER_ID", "SITE_ID", "ORDER_DATE", "SHIP_MODE")
#odf=spark.read.parquet("s3://<s3_bucket>/sqoop_output/sales_order")
odf.createOrReplaceTempView('sales_order')
odf.printSchema()

output_df=spark.sql("SELECT MKTSEGMENT, count(1) as AGGREGATE_SALES from  customer_v c, customer_site cs, sales_order so \
         WHERE c.CUST_ID = cs.CUST_ID \
         AND so.SITE_ID = cs.SITE_ID \
         GROUP by MKTSEGMENT" )
#output_df.show(10)

output_df.coalesce(1).write.mode("OVERWRITE").csv("s3://<s3_bucket>/sqoop_output/sales_segment_aggregates/")
