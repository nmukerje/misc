import json,boto3,os
from datetime import datetime, timedelta

# EMR clusters launched as far back as lookback_time_in_days
lookback_time_in_days=int(os.environ['LOOKBACK_TIME_IN_DAYS'])
# and idle for idle_time_in_mins will be terminated
idle_time_in_mins=int(os.environ['IDLE_TIME_IN_MINS'])
# unless TERMINATION_PROTECTION is switched on.
honor_termination_protection=bool(os.environ['HONOR_TERMINATION_PROTECTION'])

emr = boto3.client('emr')
cloudwatch = boto3.resource('cloudwatch')

def lambda_handler(event, context):

    # fetch EMR clusters in WAITING state
    response = emr.list_clusters(
        CreatedAfter=datetime.now() - timedelta(days=lookback_time_in_days),
        CreatedBefore=datetime.now(),
        ClusterStates=['WAITING']
    )
    clusterIds=[k['Id'] for k in response['Clusters']]
    print ("Found %d WAITING clusters : %s"%(len(clusterIds),clusterIds))
    print ("Results: ")
    
    for clusterId in clusterIds:
        try: 
            # get cluster details - for Termination Protection flag
            cluster= emr.describe_cluster(ClusterId=clusterId)
            #print (cluster)
        
            # get last CW metrics for the cluster
            metric = cloudwatch.Metric('AWS/ElasticMapReduce','AppsRunning')
            response = metric.get_statistics(Dimensions=[
              {
                'Name': 'JobFlowId',
                'Value': clusterId
               },
            ],StartTime=datetime.now() - timedelta(minutes=idle_time_in_mins),
            EndTime=datetime.now(),
            Period=1,Statistics=['Sum'])
            #print (response)
    
            # check that we have enough metrics and that AppsRunning is 0 for the idle time interval
            if (len(response['Datapoints'])==idle_time_in_mins/5 and \
                sum([k['Sum'] for k in response['Datapoints']])==0.0):
                if cluster['Cluster']['TerminationProtected']==True:
                    if honor_termination_protection==True:
                        print ("\tSkipped TerminationProtected Idle Cluster %s."%(clusterId))
                        continue
                else:
                    response = emr.set_termination_protection(
                        JobFlowIds=[clusterId],
                        TerminationProtected=False
                    ) 
            
                # Terminate EMR cluster
                emr.terminate_job_flows(JobFlowIds=[clusterId])
                print ("\tTerminated Cluster %s."%(clusterId))
        
        except Exception as e: 
            print("Exception : %s"%(e))

    return True
