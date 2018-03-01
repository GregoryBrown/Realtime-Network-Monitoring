# Cisco Realtime Networking Monitoring high level overview Wiki
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor Cisco IOS XR devices.  The application will export the desired metrics into a TSDB, to be queried and displayed.  Cisco Realtime Network Monitoring will be able to listen for alerts from opensource alerting products via webhooks, then launch the appropriate automation suite.  The

High Level Overview Diagram:
![Diagram](https://github.com/GregoryBrown/Cisco-Network-Collectors/blob/master/high-level-overview.png)

# Cisco Collectors
The collectors monitor various Cisco IOS XR devices via netconf-yang, model driven telemetry, snmp, Grpc, etc.  Given they yang model path, the collectors will export the metrics they collect to opensource TSDB such as InfluxDB, ElasticSearch, Prometheus.  

The collectors will take in a yang path, poll via GRPC, netconf-yang, dial-in telemetry, and export them to the TSDB.  Also the collectors will be able to recieve dial out telemetry data and export to a TSDB.  The collectors will be launched by the Cisco Realtime Network Monitroing tool.

# Cisco Realtime Network Monitoring Tool
This tool will give the network engineer a one stop shop for launching the Cisco collectors, monitoring alerts, monitoring automation runs, and managing infrastructure configs.  This will integrate with Cisco existing automation suites and be able to proactivly trigger jobs to resolve on device issues. 

# Timelines
![Diagram](https://github.com/GregoryBrown/Cisco-Realtime-Network-Monitoring/blob/master/High%20Level%20Timeline.png)

# Release Notes:

# TODO
* Finish uploading existing code
* Code complete of ODL collector, and MDT collector
* Testing of ODL and MDT collector. 
