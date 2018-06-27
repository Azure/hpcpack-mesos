import json
from collections import namedtuple

import requests
from requests.exceptions import HTTPError
from typing import Iterable

import logging_aux

GrowDecision = namedtuple("GrowDecision", "cores_to_grow nodes_to_grow sockets_to_grow")
IdleNode = namedtuple("IdleNode", "node_name timestamp server_name")


# TODO: change all method inputs to either all in json format or not


def _return_json_from_res(res):
    jobj = json.loads(res.content)
    return jobj


class HpcRestClient(object):
    DEFAULT_NODEGROUP_TOKEN = "DEFAULT"
    DEFAULT_COMPUTENODE_TEMPLATE = "Default ComputeNode Template"
    # auto-scale api set
    GROW_DECISION_API_ROUTE = "https://{}/HpcManager/api/auto-scale/grow-decision"
    CHECK_NODES_IDLE_ROUTE = "https://{}/HpcManager/api/auto-scale/check-nodes-idle"
    # node management api set
    BRING_NODES_ONLINE_ROUTE = "https://{}/HpcManager/api/nodes/bringOnline"
    TAKE_NODES_OFFLINE_ROUTE = "https://{}/HpcManager/api/nodes/takeOffline"
    ASSIGN_NODES_TEMPLATE_ROUTE = "https://{}/HpcManager/api/nodes/assignTemplate"
    REMOVE_NODES_ROUTE = "https://{}/HpcManager/api/nodes/remove"
    NODE_STATUS_EXACT_ROUTE = "https://{}/HpcManager/api/nodes/status/getExact"
    # node group api set
    NODE_GROUPS_ROOT_ROUTE = "https://{}/HpcManager/api/node-groups"
    LIST_NODE_GROUPS_ROUTE = NODE_GROUPS_ROOT_ROUTE
    ADD_NEW_GROUP_ROUTE = NODE_GROUPS_ROOT_ROUTE
    ADD_NODES_TO_NODE_GROUP_ROUTE = NODE_GROUPS_ROOT_ROUTE.format("{{}}") + "/{group_name}"

    # constants in result
    NODE_STATUS_NODE_NAME_KEY = "Name"
    NODE_STATUS_NODE_STATE_KEY = "NodeState"
    NODE_STATUS_NODE_STATE_ONLINE_VALUE = "Online"
    NODE_STATUS_NODE_STATE_OFFLINE_VALUE = "Offline"

    NODE_STATUS_NODE_HEALTH_KEY = "NodeHealth"
    NODE_STATUS_NODE_HEALTH_UNAPPROVED_VALUE = "Unapproved"
    NODE_STATUS_NODE_GROUP_KEY = "Groups"

    def __init__(self, hostname="localhost"):
        self.hostname = hostname
        self.logger = logging_aux.init_logger_aux("hpcframework.restclient", 'hpcframework.restclient.log')

    def _log_error(self, function_name, res):
        self.logger.error("{}: status_code:{} content:{}".format(function_name, res.status_code, res.content))

    def _log_info(self, function_name, res):
        self.logger.info(function_name + ":" + res.content)

    # TODO: consolidate these ceremonies.
    def _get(self, function_name, function_route, params):
        headers = {"Content-Type": "application/json"}
        url = function_route.format(self.hostname)
        res = requests.get(url, headers=headers, verify=False, params=params)
        try:
            res.raise_for_status()
            self._log_info(function_name, res)
            return res
        except HTTPError:
            self._log_error(function_name, res)
            raise

    def _post(self, function_name, function_route, data):
        headers = {"Content-Type": "application/json"}
        url = function_route.format(self.hostname)
        res = requests.post(url, data=data, headers=headers, verify=False)
        try:
            res.raise_for_status()
            self._log_info(function_name, res)
            return res
        except HTTPError:
            self._log_error(function_name, res)
            raise

    # Starts auto-scale api
    def get_grow_decision(self, node_group_name=""):
        url = self.GROW_DECISION_API_ROUTE.format(self.hostname)
        res = requests.post(url, verify=False)
        if res.ok:
            self.logger.info(res.content)
            grow_decision_dict = {k.upper(): v for k, v in json.loads(res.content).items()}
            if node_group_name == "":
                jobj = grow_decision_dict[self.DEFAULT_NODEGROUP_TOKEN]
            elif node_group_name.upper() in grow_decision_dict:
                jobj = grow_decision_dict[node_group_name.upper()]
            else:
                return GrowDecision(0, 0, 0)
            return GrowDecision(jobj['CoresToGrow'], jobj['NodesToGrow'], jobj['SocketsToGrow'])
        else:
            self.logger.error("status_code:{} content:{}".format(res.status_code, res.content))

    def check_nodes_idle(self, nodes):
        data = json.dumps(nodes)
        res = self._post(self.check_nodes_idle.__name__, self.CHECK_NODES_IDLE_ROUTE, data)
        jobjs = json.loads(res.content)
        return [IdleNode(idle_info['NodeName'], idle_info['TimeStamp'], idle_info['ServerName']) for idle_info in jobjs]

    # Starts node management api
    def bring_nodes_online(self, nodes):
        data = json.dumps(nodes)
        res = self._post(self.bring_nodes_online.__name__, self.BRING_NODES_ONLINE_ROUTE, data)
        return _return_json_from_res(res)

    def take_nodes_offline(self, nodes):
        data = json.dumps(nodes)
        res = self._post(self.take_nodes_offline.__name__, self.TAKE_NODES_OFFLINE_ROUTE, data)
        return _return_json_from_res(res)

    def assign_default_compute_node_template(self, nodename_arr):
        return self.assign_nodes_template(nodename_arr, self.DEFAULT_COMPUTENODE_TEMPLATE)

    def assign_nodes_template(self, nodename_arr, template_name):
        params = json.dumps({"nodeNames": nodename_arr, "templateName": template_name})
        res = self._post(self.assign_nodes_template.__name__, self.ASSIGN_NODES_TEMPLATE_ROUTE, params)
        return _return_json_from_res(res)

    def remove_nodes(self, nodes):
        data = json.dumps(nodes)
        res = self._post(self.remove_nodes.__name__, self.REMOVE_NODES_ROUTE, data)
        return _return_json_from_res(res)

    def get_node_status_exact(self, node_names):
        # type: (Iterable[str]) -> list[dict[str, any]]
        params = json.dumps({"nodeNames": node_names})
        res = self._post(self.get_node_status_exact.__name__, self.NODE_STATUS_EXACT_ROUTE, params)
        return _return_json_from_res(res)

    # Starts node group api
    def list_node_groups(self, group_name=""):
        params = {}
        if group_name != "":
            params['nodeGroupName'] = group_name
        res = self._get(self.list_node_groups.__name__, self.LIST_NODE_GROUPS_ROUTE, params)
        return _return_json_from_res(res)

    def add_node_group(self, group_name, group_description=""):
        params = json.dumps({"name": group_name, "description": group_description})
        res = self._post(self.add_node_group.__name__, self.ADD_NEW_GROUP_ROUTE, params)
        return _return_json_from_res(res)

    def add_node_to_node_group(self, group_name, node_names):
        res = self._post(self.add_node_to_node_group.__name__, self.ADD_NODES_TO_NODE_GROUP_ROUTE.format(
            group_name=group_name), json.dumps(node_names))
        return _return_json_from_res(res)


if __name__ == '__main__':
    client = HpcRestClient()
    ans = client.get_grow_decision()
    print ans.cores_to_grow
    print client.check_nodes_idle(json.dumps(['mesoswinjd']))

    print client.list_node_groups("MESOS")
    print client.add_node_group("Mesos", "Node Group for Compute nodes from Mesos")
    print client.add_node_to_node_group("mesos", ["mesoswinjd"])
    # print client.bring_nodes_online(['mesoswinjd'])
    # print client.assign_nodes_template(['iaascn000'], client.DEFAULT_COMPUTENODE_TEMPLATE)

    print client.get_node_status_exact(["mesoswinjd"])
