import requests
import json
import logging
from collections import namedtuple

GrowDecision = namedtuple("GrowDecision", "cores_to_grow nodes_to_grow sockets_to_grow")
IdleNode = namedtuple("IdleNode", "node_name idle_since")

class AutoScaleRestClient(object):
    def __init__(self, hostname="localhost"):
        self.hostname = hostname
        self.grow_decision_api_route = "https://{}/HpcManager/api/auto-scale/grow-decision"
        self.check_nodes_idle_route = "https://{}/HpcManager/api/auto-scale/check-nodes-idle"
        self.logger = logging.getLogger("hpcframwork.restclient")
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('hpcframwork.restclient.log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


    def get_grow_decision(self):
        url = self.grow_decision_api_route.format(self.hostname)
        res = requests.post(url, verify = False)
        if res.ok:
            self.logger.info(res.content)
            jobj = json.loads(res.content)
            return GrowDecision(jobj['CoresToGrow'], jobj['NodesToGrow'], jobj['SocketsToGrow'])
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))
    
    def check_nodes_idle(self, nodes):
        headers = {"Content-Type": "application/json"}
        url = self.check_nodes_idle_route.format(self.hostname)
        res = requests.post(url, data = nodes, headers = headers, verify = False)
        if res.ok:
            self.logger.info(res.content)
            jobjs = json.loads(res.content)
            return [IdleNode(idle_info['NodeName'], idle_info['IdleSince']) for idle_info in jobjs]
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))

if __name__ == '__main__':
    client = AutoScaleRestClient()
    ans = client.get_grow_decision()
    print ans.cores_to_grow
    print client.check_nodes_idle(json.dumps(['mesoswinagent', 'mesoswinagent2']))