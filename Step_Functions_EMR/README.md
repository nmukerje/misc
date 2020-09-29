Pre-requisites:

* Launch an RDS MySQL Database
* And load MySQL schema.

Files:

* Sqoop_Spark_Redshift_Pipeline.json  - Sample Step Functions Workflow with Sqoop, Glue and Spark Activities.
* emr_sqoop_bootstrap.sh - EMR Sqoop bootstrap script to install SQOOP S3 dependency.
* glue_redshift_sql.py - Glue wrapper script to execute Redshift SQL scripts.
