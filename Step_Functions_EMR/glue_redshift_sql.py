## glue_redshift_sql.py
## executes a Redshift SQL Script.
import boto3,time
import sys
import os
from pg import DB

def get_argument_value(p):
    return sys.argv[sys.argv.index(p)+1]

log_output_string=[]

def log_output(s):
    #print (s)
    log_output_string.append(s)

def executeRedshiftQuery(query, conn_string):
    log_output ("Executing Query : \n")
    start = time.time()
    log_output (query+"\n")

    response=db.query(query)
    if response:
        log_output(response)

    done = time.time()
    log_output ("Elapsed Time (in seconds) : %f \n"%(done - start))

db = DB(dbname='dev', host='<redshift host>', \
        port=5439, user='<username>', passwd='<password>')

sql_script_file=get_argument_value("--SQL_SCRIPT_FILE")
bucket_name,script_location=sql_script_file.split('/',2)[2].split('/',1)

# read the sql script file from s3
s3 = boto3.client('s3')
fileobj = s3.get_object(Bucket=bucket_name,Key=script_location)
contents = fileobj['Body'].read().decode('utf-8')

# execute the SQL queries
for sql in str(contents).split(";")[:-1]:
    sql_text = os.linesep.join([s for s in sql.splitlines() if s and not s.startswith("#")])
    response=executeRedshiftQuery(sql_text, conn_string)

## print the output to the log
print ("\n".join(log_output_string))
