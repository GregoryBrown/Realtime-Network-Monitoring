# Cisco Realtime Network AI Tool high level overview Wiki
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor Cisco IOS XR devices.  The application will export the desired metrics into a TSDB, to be queried and displayed.  Cisco Realtime AI will be able to listen for alerts from open source alerting products via web hooks, and make intelligent decisions based on the alerts to launch the appropriate automation suite. 

High Level Overview Diagram:
![Diagram](https://github.com/GregoryBrown/Cisco-Realtime-Network-Monitoring/blob/master/docs/high-level-overview.png)

# Cisco Collectors
The collectors monitor various Cisco IOS XR devices via Netconf-Yang, MDT (Model Driven Telemetry), SNMP, gRPC, etc.  Given they Yang model path, the collectors will export the metrics they collect to open source TSDB such as InfluxDB, ElasticSearch, Prometheus.  

The collectors will take in a Yang path, poll via gRPC, Netconf-Yang, Dial-In MDT, and export them to the TSDB.  Also, the collectors will be able to recieve Dial-Out MDT data and export to a TSDB.  The collectors will be launched by the Cisco Realtime Network AI tool.

# Cisco Realtime Network AI Tool
This tool will give the network engineer a one stop shop for launching the Cisco collectors, monitoring alerts, monitoring automation runs, and managing infrastructure configurations.  This will integrate with Cisco existing automation suites and be able to proactively trigger jobs to resolve on device issues based on what is failing.  The tool will be able to execute autoamtion jobs based on configurations, traffic profiles, other scripts running on box, and general stress on the box.



# TODO
* Finish uploading existing code
* Code complete of ODL collector, and MDT collector
* Testing of ODL and MDT collector.
