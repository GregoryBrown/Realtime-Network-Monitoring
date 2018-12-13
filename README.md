# Realtime Network Monitoring Tool high level overview
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor devices.  The application will export the desired metrics into a TSDB, to be queried and manage alerts. Realtime-AI will be able to listen for alerts from open source alerting products via web hooks, and make intelligent decisions based on the alerts to launch the appropriate corrections. 

# RNM Collectors (In development)
The collectors monitor various devices via Netconf-Yang, MDT (Model Driven Telemetry), SNMP, and gRPC.  Given the Yang model path, the collectors will export the metrics they collect to open source TSDB such as InfluxDB, ElasticSearch, Prometheus.  

# RNM Web App (In design)
The RNM Web App will be the one stop shop for launching the collectors, monitoring alerts, and managing infrastructure configurations. The RNM Web App will be able to plugin to automation suites that IT departments use via web hooks.

# Realtime-AI Tool (In design)
Realtime-AI tool will monitor configurations, operational states of the devices, and alerts from web hooks, and detect anomalies and peform the corrections. 
