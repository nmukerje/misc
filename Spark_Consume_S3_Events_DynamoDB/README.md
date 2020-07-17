## Consume S3 Events from DynamoDB using Spark on EMR.

#### Architecture Diagram

![Architecture Diagram](./images/spark_consume_s3_events.png?raw=true "Architecture Diagram")

### List of Resources.

1. Lambda Function : [s3-event-processor.py](lambda/s3-event-processor.py)
2. Spark Code : [s3_events_batch_process_pipeline.py](spark/s3_events_batch_process_pipeline.py)
