import base64
import codecs
import itertools
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from collections import namedtuple
from datetime import datetime

from mesoshttp.client import MesosClient
from mesoshttp.offers import Offer

import heartbeat_table
import logging_aux
import restclient
import restserver
from restclient import HpcRestClient

CHECK_IDLE_INTERVAL = 60.0
NODE_IDLE_TIMEOUT = 180.0
MESOS_NODE_GROUP_NAME = "Mesos"
MESOS_NODE_GROUP_DESCRIPTION = "The Mesos compute nodes in the cluster"


class HpcpackFramwork(object):
    class MesosFramework(threading.Thread):
        def __init__(self, client):
            threading.Thread.__init__(self)
            self.client = client
            self.stop = False

        def run(self):
            try:
                self.client.register()
            except KeyboardInterrupt:
                print('Stop requested by user, stopping framework....')

    def __init__(self, script_path="", setup_path="", headnode="", ssl_thumbprint="", framework_uri=""):
        logging.basicConfig()
        self.logger = logging_aux.init_logger_aux("hpcframework", "hpcframework.log")
        # signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.getLogger('mesoshttp').setLevel(logging.DEBUG)
        self.node_idle_check_table = {}
        self.script_path = script_path
        self.setup_path = setup_path
        self.headnode = headnode
        self.ssl_thumbprint = ssl_thumbprint
        self.framework_uri = framework_uri
        self.heartbeat_table = heartbeat_table.HeartBeatTable()
        self.hpc_client = HpcRestClient()
        self.heartbeat_table.subscribe_node_configuring(self.configure_compute_nodes_callback)
        self.heartbeat_table.start()
        self.core_provisioning = 0.0

        '''
        with open("setupscript.ps1") as scriptfile:
            hpc_setup_ps1 = scriptfile.read()
        with open("daemon.ps1") as daemonFile:
            hpc_daemon_ps1 = daemonFile.read()
        self.logger.debug("Loaded HPC daemon script:\n{}".format(hpc_setup_ps1))
        hpc_daemon_ps1_b64 = self.__encode_utf16b64(hpc_daemon_ps1)
        hpc_setup_fromed = hpc_setup_ps1.replace("$$encodedDaemonScript", '"' + hpc_daemon_ps1_b64 + '"')
        self.logger.debug("Loaded and formed HPC setup script:\n{}".format(hpc_setup_fromed))
        self.hpc_setup_ps1_b64 = self.__encode_utf16b64(hpc_setup_fromed)
        '''

        self.driver = None  # type: MesosClient.SchedulerDriver
        self.mesos_client = MesosClient(mesos_urls=['http://172.16.1.4:5050'])
        # self.mesos_client = MesosClient(mesos_urls=['zk://127.0.0.1:2181/mesos'])
        self.mesos_client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.mesos_client.on(MesosClient.OFFERS, self.offer_received)
        self.mesos_client.on(MesosClient.UPDATE, self.status_update)
        self.th = HpcpackFramwork.MesosFramework(self.mesos_client)
        self.heartbeat_server = restserver.HeartBeatServer(self.heartbeat_table, 8088)

    def start(self):
        self.th.start()
        self.heartbeat_server.start()
        self.check_runaway_and_idle_slave()
        while True and self.th.isAlive():
            try:
                self.th.join(1)
            except KeyboardInterrupt:
                self.shutdown()
                break

    def __encode_utf16b64(self, content):
        utf16 = content.encode('utf-16')
        utf16_nobom = utf16[2:] if utf16[0:2] == codecs.BOM_UTF16 else utf16
        utf16_b64 = base64.b64encode(utf16_nobom)
        return utf16_b64

    def shutdown(self):
        print 'Stop requested by user, stopping framework....'
        self.logger.warn('Stop requested by user, stopping framework....')
        self.driver.tearDown()
        self.mesos_client.stop = True
        self.stop = True
        self.heartbeat_server.stop()

    def subscribed(self, driver):
        self.logger.warn('SUBSCRIBED')
        self.driver = driver

    def status_update(self, update):
        # if update['status']['state'] == 'TASK_RUNNING':
        #     self.driver.kill(update['status']['agent_id']['value'], update['status']['task_id']['value'])
        self.logger.info("Update received:\n{}".format(str(update)))

    def offer_received(self, offers):
        try:
            # self.logger.info('OFFER: %s' % (str(offers)))
            grow_decision = self.hpc_client.get_grow_decision()
            if grow_decision is None:
                cores_to_grow = 0
            else:
                cores_in_provisioning = self.heartbeat_table.get_cores_in_provisioning()
                cores_to_grow = grow_decision.cores_to_grow - cores_in_provisioning

            for offer in offers:  # type: Offer
                take_offer = False
                if cores_to_grow > 0:
                    offer_dict = offer.get_offer()
                    self.logger.info("cores_to_grow: {}, cores_in_provisioning: {}, offer_received: {}".format(
                        cores_to_grow, cores_in_provisioning, (str(offer_dict))))
                    if 'attributes' in offer_dict:
                        attributes = offer_dict['attributes']
                        if self.get_text(attributes, 'os') == 'windows_server':
                            cores = self.get_scalar(attributes, 'cores')
                            cpus = self.get_scalar(offer_dict['resources'], 'cpus')
                            if cores == cpus:
                                if not self.heartbeat_table.check_fqdn_collision(offer_dict['hostname']):
                                    take_offer = True
                if take_offer:
                    cores_to_grow -= cpus
                    self.accept_offer(offer)
                else:
                    self.decline_offer(offer)

        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as ex:
            self.logger.exception(ex)

    def accept_offer(self, offer):
        self.logger.info("Offer %s meets HPC's requirement" % offer.get_offer()['id']['value'])
        self.run_job(offer)

    def decline_offer(self, offer):
        offer.decline()

    def get_scalar(self, collection, name):
        for i in collection:
            if i['name'] == name:
                return i['scalar']['value']
        return 0.0

    def get_text(self, collection, name):
        for i in collection:
            if i['name'] == name:
                return i['text']['value']
        return ""

    def run_job(self, mesos_offer):
        offer_dict = mesos_offer.get_offer()
        self.logger.info("Accepting offer: {}".format(str(offer_dict)))
        agent_id = offer_dict['agent_id']['value']
        fqdn = offer_dict['hostname']
        task_id = uuid.uuid4().hex
        cpus = self.get_scalar(offer_dict['resources'], 'cpus')

        task = {
            'name': 'hpc pack mesos cn',
            'task_id': {'value': task_id},
            'agent_id': {'value': agent_id},
            'resources': [
                {
                    'name': 'cpus',
                    'type': 'SCALAR',
                    # work around of MESOS-8631
                    'scalar': {'value': cpus - 0.1}
                },
                {
                    'name': 'mem',
                    'type': 'SCALAR',
                    'scalar': {'value': self.get_scalar(offer_dict['resources'], 'mem')}
                }
            ],
            'command': {'value':
                        'powershell -File ' + self.script_path + " -setupPath " + self.setup_path +
                        " -headnode " + self.headnode + " -sslthumbprint " + self.ssl_thumbprint + " -frameworkUri " + self.framework_uri + " > setupscript.log"}
        }
        self.logger.debug("Sending command:\n{}".format(task['command']['value']))
        mesos_offer.accept([task])
        self.heartbeat_table.add_slaveinfo(fqdn, agent_id, task_id, cpus)

    def _kill_task(self, host):
        self.logger.debug("Killing task {} on host {}".format(host.task_id, host.fqdn))
        self.driver.kill(host.agent_id, host.task_id)
        self.heartbeat_table.on_slave_close(host.hostname)

    def _kill_task_by_hostname(self, hostname):
        (task_id, agent_id) = self.heartbeat_table.get_task_info(hostname)
        if task_id != "":
            self.logger.debug("Killing task {} on host {}".format(task_id, hostname))
            self.driver.kill(agent_id, task_id)
            self.heartbeat_table.on_slave_close(hostname)
        else:
            self.logger.warn("Task info for host {} not found".format(hostname))

    def check_runaway_and_idle_slave(self, start_timer=True):
        (provision_timeout_list, heartbeat_timeout_list, running_list) = self.heartbeat_table.check_timeout()
        self.logger.info("Get provision_timeout_list:{}".format(str(provision_timeout_list)))
        self.logger.info("Get heartbeat_timeout_list:{}".format(str(heartbeat_timeout_list)))
        timeout_lists = [provision_timeout_list, heartbeat_timeout_list]
        for host in itertools.chain(*timeout_lists):
            self._kill_task(host)

        running_host_names = [host.hostname for host in running_list]
        idle_nodes = self.hpc_client.check_nodes_idle(json.dumps(running_host_names))
        self.logger.info("Get idle_nodes:{}".format(str(idle_nodes)))
        idle_timeout_nodes = self._check_node_idle_timeout([node.node_name for node in idle_nodes])
        self.logger.info("Get idle_timeout_nodes:{}".format(str(idle_timeout_nodes)))
        for idle_timeout_node in idle_timeout_nodes:
            self._kill_task_by_hostname(idle_timeout_node)

        if start_timer:
            timer = threading.Timer(CHECK_IDLE_INTERVAL, self.check_runaway_and_idle_slave)
            timer.daemon = True
            timer.start()

    def _check_node_idle_timeout(self, node_names):
        new_node_idle_check_table = {}
        max_tolerance = NODE_IDLE_TIMEOUT / CHECK_IDLE_INTERVAL
        for node_name in node_names:
            if node_name in self.node_idle_check_table:
                new_node_idle_check_table[node_name] = self.node_idle_check_table[node_name] + 1
            else:
                new_node_idle_check_table[node_name] = 1
        self.node_idle_check_table = new_node_idle_check_table
        return [name for name, value in self.node_idle_check_table.iteritems() if value > max_tolerance]

    def configure_compute_nodes_callback(self, configuring_node_names):
        if not configuring_node_names:
            return []

        groups = self.hpc_client.list_node_groups(MESOS_NODE_GROUP_NAME)
        if MESOS_NODE_GROUP_NAME not in groups:
            self.hpc_client.add_node_group(MESOS_NODE_GROUP_NAME, MESOS_NODE_GROUP_DESCRIPTION)

        node_status_list = self.hpc_client.get_node_status_exact(configuring_node_names)
        self.logger.info("Get node_status_list:{}".format(str(node_status_list)))
        unapproved_node_list = []
        take_offline_node_list = []
        bring_online_node_list = []
        change_node_group_node_list = []
        configured_node_names = []
        invalid_state_node_dict = {}
        for node_status in node_status_list:
            node_name = node_status["Name"]
            node_state = node_status["NodeState"]
            if node_status["NodeHealth"] == "Unapproved":
                unapproved_node_list.append(node_name)
            # node approved
            elif MESOS_NODE_GROUP_NAME not in node_status["Groups"]:                
                if node_state == "Online":
                    take_offline_node_list.append(node_name)                
                elif node_state == "Offline":
                    change_node_group_node_list.append(node_name)
                else:
                    invalid_state_node_dict[node_name] = node_state
            # node group properly set
            elif node_state == "Offline":
                bring_online_node_list.append(node_name)   
            elif node_state == "Online":
                # this node is all set
                configured_node_names.append(node_name)
            else:
                invalid_state_node_dict[node_name] = node_state
        try:
            if invalid_state_node_dict:
                self.logger.info("Node(s) in invalid state: {}".format(str(invalid_state_node_dict)))   
            if unapproved_node_list:
                self.logger.info("Assigning node template for node(s): {}".format(str(unapproved_node_list)))   
                self.hpc_client.assign_default_compute_node_template(unapproved_node_list)
            if take_offline_node_list:
                self.logger.info("Taking node(s) offline: {}".format(str(take_offline_node_list)))   
                self.hpc_client.take_nodes_offline(take_offline_node_list)
            if bring_online_node_list:
                self.logger.info("Bringing node(s) online: {}".format(str(bring_online_node_list)))   
                self.hpc_client.bring_nodes_online(bring_online_node_list)
            if change_node_group_node_list:
                self.logger.info("Changing node group node(s): {}".format(str(change_node_group_node_list)))   
                self.hpc_client.add_node_to_node_group(MESOS_NODE_GROUP_NAME, change_node_group_node_list)
        except:
            # Swallow all exceptions here. As we don't want any exception to prevent configured nodes to work
            self.logger.exception('')                        
        
        return configured_node_names


if __name__ == "__main__":  # TODO: handle various kinds of input params
    from sys import argv
    if len(argv) == 5:
        hpcpack_framework = HpcpackFramwork(argv[0], argv[1], argv[2], argv[3], argv[4])
        hpcpack_framework.start()
    else:
        hpcpack_framework = HpcpackFramwork("E:\\hpcsetup\\setupscript.ps1", "E:\\hpcsetup\\private.20180524.5b26f44.release.debug\\release.debug\\setup.exe",
                                            "mesoswinjd", "0386B1198B956BBAAA4154153B6CA1F44B6D1016", "mesoswinjd")
        
        hpcpack_framework.start()

