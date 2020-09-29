## Redshift SQL Script
COPY sales_order
FROM 's3://<s3_bucket>/sqoop_output/sales_order/'
iam_role  '<Redshift S3 IAM Role Arn>'
delimiter ',';

TRUNCATE TABLE sales_order_aggregate;

INSERT INTO sales_order_aggregate
Select ship_mode, count(1) as items_shipped from
sales_order
group by ship_mode;
