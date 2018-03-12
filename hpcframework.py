import json
import datetime
import time
import os
import sys
import threading
import logging
import signal
import sys
import uuid
import base64
import codecs
import restserver
import restclient
import logging_aux

from mesoshttp.client import MesosClient
from restclient import AutoScaleRestClient

from mesoshttp.offers import Offer

class Test(object):
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

    def __init__(self):
        logging.basicConfig()
        self.logger = logging_aux.init_logger_aux(
            "hpcframework", "hpcframework.log")
        # signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.getLogger('mesoshttp').setLevel(logging.DEBUG)

        self.hpc_client = AutoScaleRestClient()   
        self.core_provisioning = 0.0     
        with open("setupscript.ps1") as scriptfile:
            hpc_setup_ps1 = scriptfile.read()
        self.logger.info("Loaded HPC setup script:/n{}".format(hpc_setup_ps1))
        hpc_setup_ps1_utf16 = hpc_setup_ps1.encode('utf-16')
        hpc_setup_ps1_utf16_nobom = hpc_setup_ps1_utf16[2:] if hpc_setup_ps1_utf16[0:2] == codecs.BOM_UTF16 else hpc_setup_ps1_utf16
        self.hpc_setup_ps1_b64 = base64.b64encode(hpc_setup_ps1_utf16_nobom)
        

        self.driver = None  # type: MesosClient.SchedulerDriver
        self.mesos_client = MesosClient(mesos_urls=['http://172.16.1.4:5050'])
        # self.client = MesosClient(mesos_urls=['zk://127.0.0.1:2181/mesos'])
        self.mesos_client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.mesos_client.on(MesosClient.OFFERS, self.offer_received)
        self.mesos_client.on(MesosClient.UPDATE, self.status_update)
        self.th = Test.MesosFramework(self.mesos_client)
        self.th.start()
        while True and self.th.isAlive():
            try:
                self.th.join(1)
            except KeyboardInterrupt:
                self.shutdown()
                break

    def shutdown(self):
        print('Stop requested by user, stopping framework....')
        self.logger.warn('Stop requested by user, stopping framework....')
        self.driver.tearDown()
        self.mesos_client.stop = True
        self.stop = True

    def subscribed(self, driver):
        self.logger.warn('SUBSCRIBED')
        self.driver = driver

    def status_update(self, update):
        # if update['status']['state'] == 'TASK_RUNNING':
        #     self.driver.kill(update['status']['agent_id']['value'], update['status']['task_id']['value'])
        print(str(update))

    def offer_received(self, offers):
        # self.logger.info('OFFER: %s' % (str(offers)))
        grow_decision = self.hpc_client.get_grow_decision()
        
        if(grow_decision.cores_to_grow - self.core_provisioning > 0):
            for offer in offers: # type: Offer                
                self.logger.info("offer_received: {}".format(
                    (str(offer.get_offer()))))
                mesos_offer = offer.get_offer()
                if 'attributes' in mesos_offer:
                    attributes = mesos_offer['attributes']
                    if self.get_text(attributes, 'os') != 'windows_server':
                        offer.decline()
                    else:
                        cores = self.get_scalar(attributes, 'cores')
                        cpus = self.get_scalar(mesos_offer['resources'], 'cpus')

                        if cores == cpus:
                            self.accept_offer(offer)
                        else:
                            offer.decline()
                else:
                    offer.decline()
        else:
            for offer in offers:
                offer.decline()                

    def accept_offer(self, offer):
        self.logger.info("Offer %s meets hpc's requiremnt" %
                         offer.get_offer()['id']['value'])
        self.run_job(offer)

        # i = 0
        # for offer in offers:
        #     if i == 0:
        #         self.run_job(offer)
        #     else:
        #         offer.decline()
        #     i+=1
    def get_scalar(self, dict, name):
        for i in dict:
            if i['name'] == name:
                return i['scalar']['value']
        return 0.0

    def get_text(self, dict, name):
        for i in dict:
            if i['name'] == name:
                return i['text']['value']
        return ""

    def run_job(self, mesos_offer):
        offer = mesos_offer.get_offer()
        self.logger.info("Accepting offer: {}".format(str(offer)))
        task = {
            'name': 'sample test',
            'task_id': {'value': uuid.uuid4().hex},
            'agent_id': {'value': offer['agent_id']['value']},
            'resources': [
                {
                    'name': 'cpus',
                    'type': 'SCALAR',
                    'scalar': {'value': self.get_scalar(offer['resources'], 'cpus') - 0.1}
                },
                {
                    'name': 'mem',
                    'type': 'SCALAR',
                    'scalar': {'value': self.get_scalar(offer['resources'], 'mem')}
                }
            ],
            'command': {'value': 'powershell -EncodedCommand ' + self.hpc_setup_ps1_b64}
        }
        self.logger.debug("Sending command:/n{}".format(task['command']['value']))
        mesos_offer.accept([task])


if __name__ == "__main__":
    rest_server = restserver.RestServer(8088)
    server_thread = threading.Thread(target=rest_server.run)
    server_thread.start()
    test_mesos = Test()
    rest_server.stop()
    server_thread.join()
