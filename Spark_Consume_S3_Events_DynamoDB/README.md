## Consume S3 Events from DynamoDB using Spark on EMR.

#### Architecture Diagram

![Architecture Diagram](./images/spark_consume_s3_events.png?raw=true "Architecture Diagram")

### List of Resources.

1. Lambda Function : The lambda function listen to S3 file drop events and pushes the events to a DynamoDB table [s3-event-processor.py](lambda/s3-event-processor.py)
2. Spark Code : The PySpark code consumes all PENDING records from DynamoDB each run and processed them. [s3_events_batch_process_pipeline.py](spark/s3_events_batch_process_pipeline.py)

