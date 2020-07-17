## Consume S3 Events from DynamoDB using Spark on EMR.

#### Architecture Diagram

![Architecture Diagram](./images/spark_consume_s3_events.png?raw=true "Architecture Diagram")

### List of Resources.

1. Lambda Function : The lambda function listens to S3 file drop events and pushes the events to a DynamoDB table ([s3-event-processor.py](lambda/s3-event-processor.py))
2. Spark Code : The PySpark code consumes all PENDING records from DynamoDB each run and processes them. ([s3_events_batch_process_pipeline.py](spark/s3_events_batch_process_pipeline.py))

Notes:

- DynamoDB can help in reporting on these events as well but SQS, Kinesis DataStreams, Kafka etc. could also be used here. 
- Setting appropriate TTLs in the DynamoDB table can keep the table lean. [DynamoDB TTL](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/TTL.html)
- EMR Clusters launched using Step Functions with a Spark-Submit step on a schedule can ensure data get processed in a batch fashion at a certain schedule. [Step Functions EMR Integration](https://docs.aws.amazon.com/step-functions/latest/dg/connect-emr.html)
- Enabling Dynamic Allocation in Spark and Managed Scaling on EMR can ensure that both Spark and the unerlying cluster scales out to handle spikes in data volume. [Managed Scaling](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-scaling.html)
