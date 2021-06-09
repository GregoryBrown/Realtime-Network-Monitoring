# Realtime Network Monitoring Tool high level overview
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor devices.  The application will export the desired metrics into a TSDB, to be queried and manage alerts. Realtime-AI will be able to listen for alerts from open source alerting products via web hooks, and make intelligent decisions based on the alerts to launch the appropriate corrections. 

# Requirements
* Python 3.7 or higher 
* ElasticSearch 7.X or greater, or Influxdb 1.8 (2.x support coming soon)

# Installation 
1. python3.7 -m venv venv 
2. source venv/bin/activate 
3. pip install -r requirements.txt 

# How to run the application 
1. Create a configuration file that contains all of the routers you want to monitor
2. Source your virtual environment if it isn't already
3. python rtnm.py -c <config-file.ini> -b <batch size> -r <Retry if router is down or goes down>
 
```
 (venv) ott-003:~/Realtime-Network-Monitoring/rtnm > python rtnm.py -h
usage: rtnm.py [-h] -c CONFIG -b BATCH_SIZE [-w WORKER_POOL_SIZE] [-v] [-r]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Location of the configuration file
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Batch size of the upload to ElasticSearch
  -w WORKER_POOL_SIZE, --worker-pool-size WORKER_POOL_SIZE
                        Number of workers in the worker pool used for uploading
  -v, --verbose         Enable debugging
  -r, --retry           Enable retrying
 ```

  
# Sample Configuration File  
```
#For Cisco native model driven telemetry (MDT)
[Cisco-gRPC-MDT]
#required
io = input
#required, only supporting dial in right now
dial = in
#required, only supporting self-describing-gpb as of now
encoding = self-describing-gpb
#required, letting RTNM know we are using cisco native proto
format = cisco-ems
#A list of subscriptions to subscribe to on the box
subscriptions = CPU, MEMORY, DPA
address = 100.80.170.12
port = 57400
username = root
password = root
# If using TLS authenticaiton need to get the pem file off the router first
pem-file = Router.pem
# Can be set to False if you don't want to use gRPC's builtin gZip compression 
compression = True

[gNMI-MDT]
io = input
dial = in
#required, only support PROTO as of now
encoding = PROTO
format = gnmi
#Comma separated list of sensor paths to monitor
sensors =  Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters
sample-interval = 30
#Type of mode to sample, defined in the gNMI proto file
subscription-mode = SAMPLE
stream-mode = STREAM
address = 10.8.70.2
port = 57400
username = root
password = lablab
pem-file =  Router.pem
compression = True

[Output]
io = output
#required, can either be elasticsearch or influxdb
type = influxdb
address = 12.12.12.53
port = 8086
username = admin
password = password
database = db-test

```
 
![RTNM-flow](https://user-images.githubusercontent.com/365160/121397992-e2701780-c922-11eb-914d-a6b8b4b97f7d.png)

For each input section of the config file a separate process is spawned and a gRPC channel is created.
When data is flowed from the device to the processes it is added to a queue in which the main process batches the data and sends it to a worker pool for the parsing and uploading of the data.  This decoupling strategy allows RTNM to handel GBs of data a second all the while having robustness.

