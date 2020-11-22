
# Setting Yarn Node labels on an EMR Cluster to place Application Masters on OnDemand instances 

### Overview

We will explore setting Yarn Node labels on an EMR cluster to meet the following goals:

- Stability : Place Application Masters on On-Demand Instances.
- Concurrency : Allow higher concurrency of Application Masters during peak time by scaling out OnDemand nodes.
- Elasticity : And save costs by scaling in the cluster when the demand subsides.

We use a script on each node that detects whether the node is a Spot instance or an OnDemand instance and set Node Labels to Spot or OnDemand respectively. The Yarn config `yarn.node-labels.am.default-node-label-expression` allows us to specify to place Applications Masters on the OnDemand instances only.

This solution consists of

1. Configuration : Setting the right configuration on yarn-site.xml to enable node labels.
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
         "yarn.node-labels.am.default-node-label-expression":"ON_DEMAND",
         "yarn.nodemanager.node-labels.provider.configured-node-partition":"ON_DEMAND,SPOT"
      }
   },
   {
      "classification":"capacity-scheduler",
      "Properties":{
         "yarn.scheduler.capacity.root.accessible-node-labels.ON_DEMAND.capacity":"100",
         "yarn.scheduler.capacity.root.default.accessible-node-labels.ON_DEMAND.capacity":"100"
      }
   }
]
```

### Bootstrap Script

The main boostrap script is in python and is placed in S3.

#### getNodeLabels.py

```
#!/usr/bin/python3
import json
k='/mnt/var/lib/info/extraInstanceData.json'
with open(k) as f:
    response = json.load(f)
    #print ((response['instanceRole'],response['marketType']))
    if (response['instanceRole'] in ['core','task']):
       print (f"NODE_PARTITION:{response['marketType'].upper()}")
```

### Step Script

The step is placed on the EMR cluster to add the labels to Yarn on cluster startup.

#### addNodeLabels.sh

```
#!/bin/bash
sudo -u yarn yarn rmadmin -addToClusterNodeLabels "SPOT(exclusive=false),ON_DEMAND(exclusive=false)"
```

### Results







