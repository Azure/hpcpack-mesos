import base64
import codecs
import logging
import threading
import uuid

from mesoshttp.client import MesosClient
from mesoshttp.offers import Offer

import heartbeat_table
import logging_aux
import restserver
from restclient import HpcRestClient


def get_text(collection, name):
    for i in collection:
        if i['name'] == name:
            return i['text']['value']
    return ""


def get_scalar(collection, name):
    for i in collection:
        if i['name'] == name:
            return i['scalar']['value']
    return 0.0


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

    def __init__(self, script_path="", setup_path="", headnode="", ssl_thumbprint="", framework_uri="", node_group=""):
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
        self.node_group = node_group
        self.hpc_client = HpcRestClient()
        self.heartbeat_table = heartbeat_table.HpcClusterManager(self.hpc_client, node_group=self.node_group)
        self.heartbeat_table.subscribe_node_closed_callback(lambda l: map(self._kill_task_by_hostname, l))
        self.heartbeat_table.start()
        self.core_provisioning = 0.0
        self.driver = None  # type: MesosClient.SchedulerDriver
        framework_suffix = self.headnode.replace(',', '_')
        if self.node_group != "":
            framework_suffix = framework_suffix + '-' + self.node_group
        self.mesos_client = MesosClient(mesos_urls=['http://172.16.1.4:5050'],
                                        # mesos_urls=['zk://127.0.0.1:2181/mesos'],
                                        frameworkName="HPC-Pack-Framework-{}".format(framework_suffix))

        self.mesos_client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.mesos_client.on(MesosClient.OFFERS, self.offer_received)
        self.mesos_client.on(MesosClient.UPDATE, self.status_update)
        self.th = HpcpackFramwork.MesosFramework(self.mesos_client)
        self.heartbeat_server = restserver.HeartBeatServer(self.heartbeat_table, 8088)
        self.stop = False

    def start(self):
        self.th.start()
        self.heartbeat_server.start()
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
            if self.node_group == "":
                grow_decision = self.hpc_client.get_grow_decision()
            else:
                grow_decision = self.hpc_client.get_grow_decision(self.node_group)

            if grow_decision is None:
                cores_to_grow = 0
                cores_in_provisioning = 0
            else:
                cores_in_provisioning = self.heartbeat_table.get_cores_in_provisioning()
                cores_to_grow = grow_decision.cores_to_grow - cores_in_provisioning

            for offer in offers:  # type: Offer
                take_offer = False
                cpus = 0.0
                if cores_to_grow > 0:
                    offer_dict = offer.get_offer()
                    self.logger.info("cores_to_grow: {}, cores_in_provisioning: {}, offer_received: {}".format(
                        cores_to_grow, cores_in_provisioning, (str(offer_dict))))
                    if 'attributes' in offer_dict:
                        attributes = offer_dict['attributes']
                        if get_text(attributes, 'os') == 'windows_server':
                            match_node_group = False
                            if self.node_group == "":
                                match_node_group = True
                            elif get_text(attributes, 'node_group').upper() == self.node_group.upper():
                                match_node_group = True
                            else:
                                match_node_group = False

                            if match_node_group:
                                cores = get_scalar(attributes, 'cores')
                                cpus = get_scalar(offer_dict['resources'], 'cpus')
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

    def decline_offer(self, offer):
        self.logger.info("Decline offer %s" % offer.get_offer()['id']['value'])
        offer.decline()

    def accept_offer(self, offer):
        self.logger.info("Offer %s meets HPC's requirement" % offer.get_offer()['id']['value'])
        offer_dict = offer.get_offer()
        self.logger.info("Accepting offer: {}".format(str(offer_dict)))
        agent_id = offer_dict['agent_id']['value']
        fqdn = offer_dict['hostname']
        task_id = uuid.uuid4().hex
        cpus = get_scalar(offer_dict['resources'], 'cpus')

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
                    'scalar': {'value': get_scalar(offer_dict['resources'], 'mem')}
                }
            ],
            'command': {'value':
                            'powershell -File ' + self.script_path + " -setupPath " + self.setup_path +
                            " -headnode " + self.headnode + " -sslthumbprint " + self.ssl_thumbprint +
                            " -frameworkUri " + self.framework_uri + " > setupscript.log"}
        }
        self.logger.debug("Sending command:\n{}".format(task['command']['value']))
        offer.accept([task])
        self.heartbeat_table.add_slaveinfo(fqdn, agent_id, task_id, cpus)

    def _kill_task(self, host):
        self.logger.debug("Killing task {} on host {}".format(host.task_id, host.fqdn))
        self.driver.kill(host.agent_id, host.task_id)

    def _kill_task_by_hostname(self, hostname):
        (task_id, agent_id) = self.heartbeat_table.get_task_info(hostname)
        if task_id != "":
            self.logger.debug("Killing task {} on host {}".format(task_id, hostname))
            self.driver.kill(agent_id, task_id)
        else:
            self.logger.warn("Task info for host {} not found".format(hostname))


if __name__ == "__main__":  # TODO: heartbeat_uri can be optional parameter
    import argparse

    parser = argparse.ArgumentParser(description="HPC Pack Mesos framework")
    parser.add_argument("-g", "--node_group", default="")
    parser.add_argument("script_path", help="Path of HPC Pack Mesos slave setup script (e.g. setupscript.ps1)")
    parser.add_argument("setup_path", help="Path of HPC Pack setup executable (e.g. setup.exe)")
    parser.add_argument("headnode", help="Hostname of HPC Pack cluster head node")
    parser.add_argument("ssl_thumbprint",
                        help="Thumbprint of certificate which will be used in installtion and communication with HPC "
                             "Pack cluster")
    parser.add_argument("heartbeat_uri", help="Base URI of heart beat server of HPC Pack Mesos framework")
    args = parser.parse_args()

    print "Input arguments:"
    print "script_path: " + args.script_path
    print "setup_path: " + args.setup_path
    print "headnode: " + args.headnode
    print "ssl_thumbprint: " + args.ssl_thumbprint
    print "heartbeat_uri: " + args.heartbeat_uri
    if args.node_group != "":
        print "node_group: " + args.node_group

    hpcpack_framework = HpcpackFramwork(args.script_path, args.setup_path, args.headnode, args.heartbeat_uri,
                                        args.node_group)
    hpcpack_framework.start()
