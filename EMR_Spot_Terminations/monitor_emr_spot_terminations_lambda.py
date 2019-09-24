##
## monitor_emr_spot_terminations_lambda.py: 
##  Lambda function to respond to EC2 Spot Termination events
##  and push metrics to Cloudwatch.
##
import json, boto3
from datetime import date
import datetime

cw_namespace='EMR-Custom-Metrics'
cw_metric_name = 'EMRSpotTerminations'
ec2 = boto3.resource('ec2')
cloudwatch = boto3.client('cloudwatch')

def get_instance_tags(ec2,fid):
    ec2instance = ec2.Instance(fid)
    return ec2instance.tags

def lambda_handler(event, context):
    print ("Event Received : "+str(event))
    timestamp = event['time']

    # get ec2 tags
    tags = get_instance_tags(ec2,event['detail']['instance-id'])
    print (str(tags))

    ## get cluster id
    ## the value of tag aws:elasticmapreduce:job-flow-id is the cluster id
    clusterId=[k['Value'] for k in tags if k['Key'] == 'aws:elasticmapreduce:job-flow-id']
    if len(clusterId) == 1:
        clusterId = clusterId[0]
    else:
        print ("This EC2 instance is not part of an EMR Cluster.")
        return {'statusCode': 200 }

    # push Metrics to CloudWatch
    cloudwatch.put_metric_data(
        MetricData=[{'MetricName': cw_metric_name,'Dimensions': [
            {
                'Name': 'JobFlowId',
                'Value': clusterId
            },],'Timestamp': timestamp, 'Unit': 'None','Value': 1},],
        Namespace=cw_namespace
    )
    return {'statusCode': 200 }
