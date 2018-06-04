import json
import logging
from collections import namedtuple

import requests

import logging_aux

GrowDecision = namedtuple("GrowDecision", "cores_to_grow nodes_to_grow sockets_to_grow")
IdleNode = namedtuple("IdleNode", "node_name timestamp server_name")


class HpcRestClient(object):
    DEFAULT_NODEGROUP_TOKEN = "default"
    # auto-scale api set
    GROW_DECISION_API_ROUTE = "https://{}/HpcManager/api/auto-scale/grow-decision"
    CHECK_NODES_IDLE_ROUTE = "https://{}/HpcManager/api/auto-scale/check-nodes-idle"
    # node management api set
    BRING_NODE_ONLINE_ROUTE = "https://{}/HpcManager/api/nodes/bringOnline"

    def __init__(self, hostname="localhost"):
        self.hostname = hostname
        self.logger = logging_aux.init_logger_aux("hpcframework.restclient", 'hpcframework.restclient.log')

    def _log_error(self, function_name, res):
            self.logger.error("{}: status_code:{} content:{}".format(function_name, res.status_code, res.content))       

    def _log_info(self, function_name, res):
        self.logger.info(function_name + ":" + res.content)

    def _post(self, function_name, function_route, data):
        headers = {"Content-Type": "application/json"}
        url = function_route.format(self.hostname)
        res = requests.post(url, data=data, headers=headers, verify=False)
        if res.ok:
            self._log_info(function_name, res)
            return True, res
        else:
            self._log_error(function_name, res)
            return False, None

    def get_grow_decision(self):
        url = self.GROW_DECISION_API_ROUTE.format(self.hostname)
        res = requests.post(url, verify=False)
        if res.ok:
            self.logger.info(res.content)
            jobj = json.loads(res.content)[self.DEFAULT_NODEGROUP_TOKEN]
            return GrowDecision(jobj['CoresToGrow'], jobj['NodesToGrow'], jobj['SocketsToGrow'])
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))

    def check_nodes_idle(self, nodes):
        success, res = self._post(self.check_nodes_idle.__name__, self.CHECK_NODES_IDLE_ROUTE, nodes)        
        if success:            
            jobjs = json.loads(res.content)
            return [IdleNode(idle_info['NodeName'], idle_info['TimeStamp'], idle_info['ServerName']) for idle_info in jobjs]
    

    def bring_node_online(self, nodes):
        success, res = self._post(self.bring_node_online.__name__, self.BRING_NODE_ONLINE_ROUTE, nodes)
        if success:
            jobj = json.loads(res.content)
            return jobj

if __name__ == '__main__':
    client = HpcRestClient()
    ans = client.get_grow_decision()
    print ans.cores_to_grow
    print client.check_nodes_idle(json.dumps(['mesoswinjd']))
    
    print client.bring_node_online(json.dumps(['mesoswinjd']))
