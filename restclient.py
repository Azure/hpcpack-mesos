import json
import logging
from collections import namedtuple

import requests

import logging_aux

GrowDecision = namedtuple("GrowDecision", "cores_to_grow nodes_to_grow sockets_to_grow")
IdleNode = namedtuple("IdleNode", "node_name timestamp server_name")


class HpcRestClient(object):
    def __init__(self, hostname="localhost"):
        self.hostname = hostname
        self.grow_decision_api_route = "https://{}/HpcManager/api/auto-scale/grow-decision"
        self.check_nodes_idle_route = "https://{}/HpcManager/api/auto-scale/check-nodes-idle"
        self.logger = logging_aux.init_logger_aux("hpcframework.restclient", 'hpcframework.restclient.log')

    def get_grow_decision(self):
        url = self.grow_decision_api_route.format(self.hostname)
        res = requests.post(url, verify=False)
        if res.ok:
            self.logger.info(res.content)
            jobj = json.loads(res.content)
            return GrowDecision(jobj['CoresToGrow'], jobj['NodesToGrow'], jobj['SocketsToGrow'])
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))

    def check_nodes_idle(self, nodes):
        headers = {"Content-Type": "application/json"}
        url = self.check_nodes_idle_route.format(self.hostname)
        res = requests.post(url, data=nodes, headers=headers, verify=False)
        if res.ok:
            self.logger.info("check_nodes_idle:" + res.content)
            jobjs = json.loads(res.content)
            return [IdleNode(idle_info['NodeName'], idle_info['TimeStamp'], idle_info['ServerName']) for idle_info in jobjs]
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))


if __name__ == '__main__':
    client = HpcRestClient()
    ans = client.get_grow_decision()
    print ans.cores_to_grow
    print client.check_nodes_idle(json.dumps(['mesoswinagent', 'mesoswinagent2']))
