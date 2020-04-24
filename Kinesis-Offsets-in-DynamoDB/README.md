
This sample code demonstrates a simple mechanism to store Kinesis checkpoints (offsets) in DynamoDB.

## PreReqs:

* Create a DynamoDB table 'spark-streaming-demo' with HASH key 'leaseKey' and RANGE key 'batch'. This table essentially serves as a clone of the DynamoDB table that the Kinesis Consumer writes.

* Create a Kinesis datastream called 'spark-streaming-demo' is us-west-2.

## How it works:

- The sample code reads from the Kinesis datastream in 30 second batches.
- For each batch read, the KCL will store checkpoints in the DynamoDB table - spark-streaming-demo.
- After processing, the 'DynamoDBUtils.saveCheckPoint' will copy the checkpoint over to the checkpoint DynamoDB table - spark-streaming-demo-checkpoint.
- When the application is restarted, the 'DynamoDBUtils.restoreCheckPoint' will copy the last processed checkpoint back to the orignal KCL DynamoDB table.

To test it:

- Build the Spark Jar from the code.
- Start the Spark Application
```
spark-submit  --class main.scala.SparkKinesisTest Spark-Kinesis-Test-assembly-0.1.jar
```
- Let the 1st batch complete and observe the checkpoints in the DynamoDB tables.
- For the 2nd batch, observe from the Spark Streaming UI and kill the application when the batch starts reading records.
- The KCL DynamoDB table will have the latest read offsets while the checkpoint table will have the last processed offsets.
- Now restart the application, the last processed offsets from the the checkpoint table will be written back to the  KCL DynamoDB table.
- This will allow gracefull failure without any loss of data.
- This will also ensure at least once processing of each the data in the Kinesis Datastream.

