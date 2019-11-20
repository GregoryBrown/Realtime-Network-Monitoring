# Realtime Network Monitoring Tool high level overview
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor devices.  The application will export the desired metrics into a TSDB, to be queried and manage alerts. Realtime-AI will be able to listen for alerts from open source alerting products via web hooks, and make intelligent decisions based on the alerts to launch the appropriate corrections. 

# Requirements
* Python 3.7 or higher 
* Multi-Core machine
* ElasticSearch 7.X or greater

# Installation 
1. python3.7 -m venv venv 
2. source venv/bin/activate 
3. pip install -r requirements.txt 

# Configuration File Sample 
```
[dial-in-cisco-ems]
#Required
io = input
dial = in
encoding = kvgpb
format = cisco-ems
#Required for cisco-ems, can be a , separated list
subs = DPA
address = 10.8.70.42
port = 57400
username = root
password = lablab
#Optional
pem-file = Test.pem
batch-size = 200

[dial-in-gnmi]
#input or output
io = input
#in or out
dial = in
#kvgpb only supported now 
encoding = kvgpb
#gnmi, cisco-ems
format = gnmi
#Required for gNMI, can be a , separated list
sensors = Cisco-IOS-XR-ethernet-lldp-oper:lldp/nodes/node/neighbors/devices/device
#Required for gNMI
sample-interval = 10
#Required
address = 10.8.99.5
#Required
port = 57400
#Requried 
username = root
#Required
password = lablab 
#Optional
pem-file = Test.pem 
batch-size = 10
#sample, on-change required for gNMI
sub-mode = sample 
#stream, poll, once required for gNMI
stream-mode = stream

[dial-out]
io = input
#Required
dial = out
#Required
address = 0.0.0.0 
#Required
port = 5432
#Number greater than 0
batch-size = 25

#Only supports Elasticsearch as of now
[elasticsearch-server-1] 
io = output
#Required
address = 2.2.2.1
#Required
port = 9200 

```


