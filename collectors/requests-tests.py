from requests import request
import json

def main():
    payload = [{ "index" : {'_index': 'cisco-ios-xr-pfi-im-cmd-oper:interfaces-interface-xr-interface', '_type': 'nodes'}},{"keys": [{"interface-name": "tunnel-te2112"}], "content": [{"interface-handle": "tunnel-te2112", "interface-type": "IFT_TUNNEL_TE", "hardware-type-string": "Tunnel-TE", "state": "im-state-down"}]},{ "index" : {'_index': 'cisco-ios-xr-pfi-im-cmd-oper:interfaces-interface-xr-interface', '_type':'nodes'} },{"keys": [{"interface-name": "tunnel-te2112"}], "content": [{"interface-handle": "tunnel-te2112", "interface-type": "IFT_TUNNEL_TE", "hardware-type-string": "Tunnel-TE", "state": "im-state-down"}]}]
    print(type(payload[0]))
    data_to_post = '\n'.join(json.dumps(d) for d in payload)
    data_to_post += '\n'
    index_url = f"http://web-ott-tsdb-server-1:9200/_bulk"
    headers = {'Content-Type': "application/x-ndjson"}
    reply = request("POST", index_url, data=data_to_post, headers=headers)
    print(data_to_post)
    print(reply.json())





if __name__ =='__main__':
    main()

