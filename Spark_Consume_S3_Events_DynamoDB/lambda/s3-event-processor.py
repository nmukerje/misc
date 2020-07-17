import boto3

# The DynamoDB table name
dynamodb_tablename = 'S3_File_Monitor'
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

def lambda_handler(event, context):

    print ('Event : '+str(event))
    table=dynamodb.Table(dynamodb_tablename)
    
    for e in event['Records']:
        # parse S3 event
        eventTime=e['eventTime'] 
        objectKey=e['s3']['object']['key']
        bucket=e['s3']['bucket']['name']
        print (bucket)
        
        # save record to DynamoDB
        response = table.put_item(
          Item={
              'objectKey': objectKey,
              'eventTime': eventTime,
              'bucket': bucket,
              'status':'PENDING'
          })
        print (response)

    return True
