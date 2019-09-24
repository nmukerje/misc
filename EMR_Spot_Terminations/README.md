# Monitor EMR Spot Instance Terminations in CloudWatch Dashboard using AWS Lambda:


## Deploy a Lambda Function that will respond to EC2 Spot Termination Cloudwatch Events.

The lambda function code in is [monitor_emr_spot_terminations_lambda.py](https://github.com/nmukerje/misc/blob/master/EMR_Spot_Terminations/monitor_emr_spot_terminations_lambda.py)

## Configure an CloudWatch Rule to trigger the lambda function on Spot Termination messages.

From the Cloudwatch console, setup a rule to trigger the lambda function on EC2 Spot Termination events:

![Cloudwatch Event Rule](https://github.com/nmukerje/misc/blob/master/EMR_Spot_Terminations/ec2_spot-termination_cw_rule.png)

## Build a Cloudwatch Widget specific for each EMR Cluster to monitor Spot Terminations.

The final step to use build a Cloudwatch Metrics dashboard/widget to view the Spot Termination metrics:

![Cloudwatch Metrics Dashboard](https://github.com/nmukerje/misc/blob/master/EMR_Spot_Terminations/cw_chart.png)
