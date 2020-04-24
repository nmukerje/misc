# Checkpointing in DynamoDB for Spark Dstream applications reading from Kinesis Data Streams.

The sample code demonstrates a simple solution to store Kinesis processed checkpoints (offsets) in DynamoDB.

## PreReqs:

* Create a DynamoDB table 'spark-streaming-demo' with HASH key 'leaseKey' and RANGE key 'batch'. This table essentially serves as a clone of the DynamoDB table that the Kinesis Consumer Library (KCL) creates.
* Create a Kinesis datastream called 'spark-streaming-demo' in us-west-2.

## How it works:

- The sample code reads from the Kinesis datastream in 30 second batches.
- For each batch read, the KCL will store checkpoints in the DynamoDB table - spark-streaming-demo.
- After processing, the 'DynamoDBUtils.saveCheckPoint' will copy the checkpoint over to the checkpoint DynamoDB table - spark-streaming-demo-checkpoint.
- When the application is restarted, the 'DynamoDBUtils.restoreCheckPoint' will copy the last processed checkpoint back to the orignal KCL DynamoDB table to ensure that reading resumes from after the data last processed.

## To test it:

- Build the Spark Jar from the code.
- Start the Spark Application
```
spark-submit  --class main.scala.SparkKinesisTest Spark-Kinesis-Test-assembly-0.1.jar
```
- Let the 1st batch complete and observe the checkpoints in the DynamoDB tables.
- For the 2nd batch, observe from the Spark Streaming UI and kill the application when the batch starts reading records.
- The KCL DynamoDB table will have the latest read offsets from the 2nd batch while the checkpoint table will have the last processed offsets from the previous batch.
- Now restart the application, the last processed offsets from the the checkpoint table will be written back to the  KCL DynamoDB table and the 2nd batch will be re-read to be processed.

This allows graceful failure of Spark Dstreams Applications without loss of data (without using HDFS or S3 checkpointing) and ensures at least once processing of the data in the Kinesis Datastream.
