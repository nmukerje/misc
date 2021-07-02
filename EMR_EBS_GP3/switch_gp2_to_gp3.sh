#!/bin/bash
# get GP2 volumes
gp2_volumes=$(aws ec2 describe-volumes --filters Name=attachment.instance-id,Values=`curl -s http://169.254.169.254/latest/meta-data/instance-id` | jq -r '.Volumes[] | select(.VolumeType=="gp2").VolumeId')

for v in $gp2_volumes
do
	echo "Switching $v from GP2 to GP3."
	# Switch them to GP3
	aws ec2 modify-volume --volume-type gp3 --volume-id $v
done
