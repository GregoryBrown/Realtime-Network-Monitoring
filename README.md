# Realtime Network Monitoring Tool high level overview
The goal of the application is to be a unified collection system of all the different technologies that are able to monitor devices.  The application will export the desired metrics into a TSDB, to be queried and manage alerts. Realtime-AI will be able to listen for alerts from open source alerting products via web hooks, and make intelligent decisions based on the alerts to launch the appropriate corrections. 

# Requirements
* Python 3.7 or higher 
* Multi-Core machine

# Installation 
1. python3.7 -m venv venv 
2. source venv/bin/activate 
3. pip install -r requirements.txt 
