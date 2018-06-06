import json
import logging
from collections import namedtuple

import requests

import logging_aux

GrowDecision = namedtuple("GrowDecision", "cores_to_grow nodes_to_grow sockets_to_grow")
IdleNode = namedtuple("IdleNode", "node_name timestamp server_name")


class HpcRestClient(object):
    DEFAULT_NODEGROUP_TOKEN = "default"
    DEFAULT_COMPUTENODE_TEMPLATE = "Default ComputeNode Template"
    # auto-scale api set
    GROW_DECISION_API_ROUTE = "https://{}/HpcManager/api/auto-scale/grow-decision"
    CHECK_NODES_IDLE_ROUTE = "https://{}/HpcManager/api/auto-scale/check-nodes-idle"
    # node management api set
    BRING_NODES_ONLINE_ROUTE = "https://{}/HpcManager/api/nodes/bringOnline"
    TAKE_NODES_OFFLINE_ROUTE = "https://{}/HpcManager/api/nodes/takeOffline"
    ASSIGN_NODES_TEMPLATE_ROUTE = "https://{}/HpcManager/api/nodes/assignTemplate"
    REMOVE_NODES_ROUTE = "https://{}/HpcManager/api/nodes/remove"

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

    # Starts auto-scale api
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

    # Starts node management api
    def bring_nodes_online(self, nodes):
        success, res = self._post(self.bring_nodes_online.__name__, self.BRING_NODES_ONLINE_ROUTE, nodes)
        if success:
            jobj = json.loads(res.content)
            return jobj

    def take_nodes_offline(self, nodes):
        success, res = self._post(self.take_nodes_offline.__name__, self.TAKE_NODES_OFFLINE_ROUTE, nodes)
        if success:
            jobj = json.loads(res.content)
            return jobj

    def assign_nodes_template(self, nodename_arr, template_name):
        params = json.dumps({"nodeNames": nodename_arr, "templateName": template_name})
        success, res = self._post(self.assign_nodes_template.__name__, self.ASSIGN_NODES_TEMPLATE_ROUTE, params)
        if success:
            jobj = json.loads(res.content)
            return jobj

    def remove_nodes(self, nodes):        
        success, res = self._post(self.remove_nodes.__name__, self.REMOVE_NODES_ROUTE, nodes)
        if success:
            jobj = json.loads(res.content)
            return jobj


if __name__ == '__main__':
    client = HpcRestClient()
    ans = client.get_grow_decision()
    print ans.cores_to_grow
    print client.check_nodes_idle(json.dumps(['mesoswinjd']))

    # print client.bring_nodes_online(json.dumps(['mesoswinjd']))
    # print client.assign_nodes_template(['iaascn000'], client.DEFAULT_COMPUTENODE_TEMPLATE)
