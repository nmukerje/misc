
# Setting Yarn Node labels on an EMR Cluster to place Application Masters on OnDemand instances 

### Overview

We will explore setting Yarn Node labels on an EMR cluster to meet the following goals for Application Masters:

- Stability : Place Application Masters on On-Demand Instances.
- Concurrency : Allow higher concurrency of Application Masters during peak time by scaling out OnDemand nodes.
- Elasticity : And save costs by scaling in the cluster when the demand subsides.

The config `yarn.nodemanager.node-labels.provider` allows us to use a script on each node that detects whether the node is a Spot instance or an OnDemand instance and set Node Labels to SPOT or ON_DEMAND respectively. The Yarn config `yarn.node-labels.am.default-node-label-expression` allows us to specify to place Applications Masters on the ON_DEMAND node labels only.

This solution consists of

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

Now if we launch a cluster with the above config and bootstrap action, we should see the Yarn Node Labels set correctly.

![yarn_node_labels](./images/yarn_node_labels.png =250x)







