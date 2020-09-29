## Redshift SQL Script 2
COPY sales_segment_aggregates
FROM 's3://<s3_bucket>/sqoop_output/sales_segment_aggregates/'
iam_role  '<Redshift_S3_Role>'
delimiter ',';
