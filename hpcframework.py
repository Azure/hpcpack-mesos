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

from mesoshttp.client import MesosClient

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
        self.logger = logging.getLogger(__name__)
        #signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.getLogger('mesoshttp').setLevel(logging.DEBUG)
        self.driver = None
        self.client = MesosClient(mesos_urls=['http://172.16.1.4:5050'])
        #self.client = MesosClient(mesos_urls=['zk://127.0.0.1:2181/mesos'])
        self.client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.client.on(MesosClient.OFFERS, self.offer_received)
        self.client.on(MesosClient.UPDATE, self.status_update)
        self.th = Test.MesosFramework(self.client)
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
        self.client.stop = True
        self.stop = True


    def subscribed(self, driver):
        self.logger.warn('SUBSCRIBED')
        self.driver = driver

    def status_update(self, update):
        # if update['status']['state'] == 'TASK_RUNNING':
        #     self.driver.kill(update['status']['agent_id']['value'], update['status']['task_id']['value'])
        pass

    def offer_received(self, offers):
        # self.logger.warn('OFFER: %s' % (str(offers)))

        for offer in offers:
            print(str(offer.get_offer()))
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
                

    def accept_offer(self, offer):
        print("Offer %s meets hpc's requiremnt" % offer.get_offer()['id']['value'])
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
        print(str(offer))
        task = {
            'name': 'sample test',
            'task_id': {'value': uuid.uuid4().hex},
            'agent_id': {'value': offer['agent_id']['value']},
            'resources': [
            {
                'name': 'cpus',
                'type': 'SCALAR',
                'scalar': {'value': 3.9}
            },
            {
                'name': 'mem',
                'type': 'SCALAR',
                'scalar': {'value': 1000}
            }
            ],
            'command': {'value': 'powershell c:\\HPCPack2016\\5.1.6086.0\\setupscript.ps1'}            
        }

        mesos_offer.accept([task])

test_mesos = Test()
