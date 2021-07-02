
### Switch EMR Cluster gp2 volumes to gp3

>  gp3 is the next-generation general purpose SSD volumes for Amazon Elastic Block Store (Amazon EBS) that enable customers to provision performance independent of storage capacity and provides up to 20% lower price-point per GB than existing gp2 volumes. With gp3 volumes, customers can scale IOPS (input/output operations per second) and throughput without needing to provision additional block storage capacity, and pay only for the resources they need.  

Ensure that the EMR Cluster has gp2 volumes attached. Attach the bootstrap script `switch_gp2_to_gp3.sh` to your EMR Cluster. 

```
#!/bin/bash

# get gp2 volumes
gp2_volumes=$(aws ec2 describe-volumes --filters Name=attachment.instance-id,Values=`curl -s http://169.254.169.254/latest/meta-data/instance-id` | jq -r '.Volumes[] | select(.VolumeType=="gp2").VolumeId')

for v in $gp2_volumes
do
	echo "Switching $v from gp2 to gp3."
	# Switch the volumes to gp3
	aws ec2 modify-volume --volume-type gp3 --volume-id $v
done

``` 

The scripts detects the gp2 volumes attached to your EMR Cluster and invokes the commands to switch them to gp3.

Note: You can change it but the script does not override the baseline gp3 throughput and IOPS settings so you get the minimum 125MB/s and 3000 IOPs per volume (which is already the maximum IOPS for a gp2 volume).
