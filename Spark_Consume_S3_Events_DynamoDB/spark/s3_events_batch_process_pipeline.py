from pyspark.sql.functions import col
import boto3 
from pyspark.sql import SparkSession

# DynamoDB Table and Column Names
tableName='S3_File_Monitor'
columns=['objectKey','eventTime','bucket','status']

DynamoDBInputFormat="org.apache.hadoop.dynamodb.read.DynamoDBInputFormat"
Text="org.apache.hadoop.io.Text"
DynamoDBItemWritable="org.apache.hadoop.dynamodb.DynamoDBItemWritable"

# Marks S3 events in Dynamodb as PROCESSED
def mark_as_processed(x):
    table = boto3.resource('dynamodb', region_name='us-east-2').Table(tableName)

    response=table.update_item(
       Key={'objectKey':x[0],
            'eventTime':x[1]},
       AttributeUpdates={'status': {"Value": "PROCESSED" },
       }
    )
    print (response) 

# Initialize Spark Session
spark=SparkSession.builder.appName("Process_PENDING_S3_Events").enableHiveSupport().getOrCreate()
    
jobConf={
    "dynamodb.servicename":"dynamodb",
    "dynamodb.input.tableName":"S3_File_Monitor",
    "dynamodb.endpoint":"dynamodb.us-east-2.amazonaws.com",
    "dynamodb.regionid":"us-east-2",
    "dynamodb.throughput.read.percent":"1.0",
    "dynamodb.version":"2011-12-05",
    "dynamodb.output.tableName":"S3_File_Monitor",
    "dynamodb.throughput.write.percent":"1.0",
    "mapred.output.format.class":"org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat",
    "mapred.input.format.class":"org.apache.hadoop.dynamodb.read.DynamoDBInputFormat"
}

# Read PENDING Records
s3_events = spark.sparkContext.hadoopRDD(inputFormatClass=DynamoDBInputFormat,
                      keyClass=Text,
                      valueClass=DynamoDBItemWritable,
                      conf=jobConf )

df=s3_events.map(lambda x: tuple([x[1]['item'][c]['s'] for c in columns]))\
   .toDF(columns).filter(col("status") == "PENDING").cache()
df.show(5,False)
print (df.count())

filepaths=df.select("bucket","objectKey").rdd.map(lambda x: "s3://"+x[0]+"/"+x[1]).collect()
print (filepaths)

input_df=spark.read.parquet(*filepaths)
input_df.printSchema()
input_df.count()

## Transformations
input_df.coalesce(1).write.mode("OVERWRITE").orc("s3://<bucket>/orc_output/")

## Mark records as PROCESSED
df.select("objectKey","eventTime").rdd.foreach(lambda x : mark_as_processed(x))
