
### Switch your EMR Cluster GP2 volumes to GP3

Step 1:

Ensure that the EMR Cluster has GP2 volumes attached. Attach the bootstrap script `switch_gp2_to_gp3.sh` to your EMR Cluster. 

```
#!/bin/bash

gp2_volumes=$(aws ec2 describe-volumes --filters Name=attachment.instance-id,Values=`curl -s http://169.254.169.254/latest/meta-data/instance-id` | jq -r '.Volumes[] | select(.VolumeType=="gp2").VolumeId')

for v in $gp2_volumes
do
        echo "Switching $v from GP2 to GP3."
        aws ec2 modify-volume --volume-type gp3 --volume-id $v
done

``` 

The scripts detects the GP2 volumes attached to your EMR Cluster and invoked the commands to switch them to GP3.

Note: You can change it but the script does not specify the GP3 throughput and IOPS settings so you get the baseline 125MB/s and 3000 IOPs (which is more than GP2).
