
# Setting Yarn Node labels on an EMR Cluster to place Application Masters on OnDemand instances 

### Overview

We will explore setting Yarn Node labels on an EMR cluster to meet the following goals for Application Masters:

- <b>Stability</b> : Place Application Masters on On-Demand Instances only.
- <b>Concurrency</b> : Allow higher concurrency of Application Masters during peak time by scaling out OnDemand nodes.
- <b>Elasticity</b> : And save costs by scaling in the cluster when the demand subsides.

The config `yarn.nodemanager.node-labels.provider` allows us to use a script on each node that detects whether the node is a Spot instance or an OnDemand instance and set Node Labels to SPOT or ON_DEMAND respectively. The Yarn config `yarn.node-labels.am.default-node-label-expression` allows us to specify to place Applications Masters on the ON_DEMAND node labels only.

This solution consists of the following:

1. Configuration : Setting the right configuration on yarn-site.xml to enable node labels to bootup.
2. Bootstrap Script : To detect and set Node Labels correctly for On-Demand and Spot Instances.
3. Step Script : To add the node labels on cluster startup.

### Configuration

Here is the configuration json to set the right properties on yarn-site.xml. 

```
[
   {
      "classification":"yarn-site",
      "Properties":{
         "yarn.nodemanager.node-labels.provider":"script",
         "yarn.nodemanager.node-labels.provider.script.path":"/home/hadoop/getNodeLabels.py",
         "yarn.node-labels.enabled":"true",
         "yarn.node-labels.am.default-node-label-expression":"ON_DEMAND"
      }
   }
]
```

### Bootstrap Script

The main work to detect the instance type and return the Node Labels is done in python and is placed in S3 and is invoked by our boostrap script.

#### getNodeLabels.py

```
#!/usr/bin/python3
import json
k='/mnt/var/lib/info/extraInstanceData.json'
with open(k) as f:
    response = json.load(f)
    if (response['instanceRole'] in ['core','task']):
       print (f"NODE_PARTITION:{response['marketType'].upper()}")
```

#### getNodeLabels_bootstrap.sh
```
#!/bin/bash
aws s3 cp s3://<bucket>/yarn_nodel_labels/getNodeLabels.py /home/hadoop
chmod +x /home/hadoop/getNodeLabels.py
```

### Step Script

The step is placed on the EMR cluster to add the labels to Yarn on cluster startup.

#### addNodeLabels.sh

```
#!/bin/bash
sudo -u yarn yarn rmadmin -addToClusterNodeLabels "SPOT(exclusive=false),ON_DEMAND(exclusive=false)"
```

### Results

Now if we launch a cluster with the above config, bootstrap action and step, we should see the Yarn Node Labels set correctly on each node.

<img src="https://raw.githubusercontent.com/nmukerje/misc/master/EMR_Yarn_Node_Labels/images/yarn_node_labels.png" alt="yarn_node_labels" width="400"/>







