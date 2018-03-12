from flask import Flask, request
app = Flask(__name__)
from requests import request as rrequest
import json

def configure_backup():
    url = "http://sit-emr-odl:8181/restconf/config/network-topology:network-topology/topology/topology-netconf/node/NCS5501-SE-LER1/yang-ext:mount/Cisco-IOS-XR-clns-isis-cfg:isis/instances/instance/WEB/interfaces/interface/Bundle-Ether22"
    payload = '''<interface xmlns="http://cisco.com/ns/yang/Cisco-IOS-XR-clns-isis-cfg">
       <interface-name>Bundle-Ether22</interface-name>
       <running></running>
       <interface-afs>
        <interface-af>
         <af-name>ipv4</af-name>
         <saf-name>unicast</saf-name>
         <interface-af-data>
          <running></running>
         </interface-af-data>
        </interface-af>
        <interface-af>
         <af-name>ipv6</af-name>
         <saf-name>unicast</saf-name>
         <interface-af-data>
          <running></running>
         </interface-af-data>
        </interface-af>
       </interface-afs>
      </interface>'''
    headers = {
        'Content-Type': "application/xml",
        'Authorization': "Basic YWRtaW46YWRtaW4=",
        'Cache-Control': "no-cache",
        'Postman-Token': "60cf02eb-8fa5-484a-af42-d6bd045a3774"
    }
    response = rrequest("PUT", url, data=payload, headers=headers)
    print(response.text)


@app.route('/', methods=['POST'])
def test_alert():
    data = json.loads(request.data.decode('utf-8'))
    if data["state"] == 'alerting':
        configure_backup()
    return 'DONE'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8484)


