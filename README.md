# Cisco Realtime Networking Monitoring high level overview
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor Cisco IOS XR devices.  The application will export the desired metrics into a TSDB, to be queried and displayed.  This application will also allow the triggering of notifications based on alerts in the metrics

High Level Overview Diagram:
![Diagram](https://github.com/GregoryBrown/Cisco-Network-Collectors/blob/master/high-level-overview.png)

# Cisco Collectors
The collectors monitor various Cisco IOS XR devices via netconf-yang, model driven telemetry, snmp, Grpc, etc.  Given they yang model path, the collectors will export the metrics they collect to opensource TSDB such as InfluxDB, ElasticSearch, Prometheus.  

# Timelines
![Diagram](https://github.com/GregoryBrown/Cisco-Realtime-Network-Monitoring/blob/master/High%20Level%20Timeline.png)
