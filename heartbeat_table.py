import threading
from collections import namedtuple
from datetime import datetime, timedelta

import logging_aux


class HeartBeatTable(object):

    def __init__(self, provisioning_timeout=timedelta(minutes=15), heartbeat_timeout=timedelta(minutes=3)):
        self._table = {}
        self.logger = logging_aux.init_logger_aux("hpcframework.heartbeat", "hpcframework.heartbeat.log")
        self.on_host_running = []
        self._table_lock = threading.Lock()
        self._provisioning_timeout = provisioning_timeout
        self._heartbeat_timeout = heartbeat_timeout

    def add_slaveinfo(self, hostname, agent_id, task_id, cpus, last_heartbeat=datetime.utcnow()):
        if hostname in self._table and self._table[hostname].state != HpcState.Closed:
            self.logger.warn("Heart beat entry of {} existed. old value: {}.".format(
                hostname, str(self._table[hostname])))
        slaveinfo = SlaveInfo(hostname, agent_id, task_id, cpus, last_heartbeat, HpcState.Provisioning)
        self._table[hostname] = slaveinfo
        self.logger.info("Heart beat entry added: {}".format(str(slaveinfo)))

    def on_slave_heartbeat(self, hostname):
        if hostname in self._table:
            self._table[hostname].last_heartbeat = datetime.utcnow()
            self.logger.info("Heatbeat from host {}".format(hostname))
            if self._table[hostname].state == HpcState.Provisioning:
                with self._table_lock:  # to ensure we only run running callback once per entry
                    if self._table[hostname].state == HpcState.Provisioning:
                        self._table[hostname].state = HpcState.Running
                        self.__exec_callback(self.on_host_running)
                        self.logger.info("Host {} start running".format(hostname))
        else:
            self.logger.error("Host {} is not recognized. Heartbeat ignored.".format(hostname))

    def on_slave_close(self, hostname):
        if hostname in self._table:
            self._table[hostname].state = HpcState.Closed
            self.logger.info("Host {} closed".format(hostname))
        else:
            self.logger.error("Host {} is not recognized. Close event ignored.".format(hostname))

    def get_task_info(self, hostname):
        if hostname in self._table:
            entry = self._table[hostname]
            return (entry.task_id, entry.agent_id)
        else:
            self.logger.error("Host {} is not recognized. Failed to get task info.".format(hostname))

    def __exec_callback(self, callbacks):
        for callback in callbacks:
            try:
                self.logger.debug('Callback %s on %s' % (callback.__name__))
                callback()
            except Exception as e:
                self.logger.exception('Error in %s callback: %s' % (callback.__name__, str(e)))

    def check_timeout(self, now=datetime.utcnow()):
        provision_timeout_list = []
        heartbeat_timeout_list = []
        running_list = []
        for host in dict(self._table).itervalues():
            if host.state == HpcState.Provisioning and now - host.heartbeat_timeout >= self._provisioning_timeout:
                provision_timeout_list.append(host)
            elif host.state == HpcState.Running:
                if now - host.heartbeat_timeout >= self._heartbeat_timeout:
                    heartbeat_timeout_list.append(host)
                else:
                    running_list.append(host)
        return (provision_timeout_list, heartbeat_timeout_list, running_list)

    def get_cores_in_provisioning(self):
        cores = 0.0
        for host in dict(self._table).itervalues():
            if host.state == HpcState.Provisioning:
                cores += host.cpus
        return cores


SlaveInfo = namedtuple(
    "SlaveInfo", "hostname agent_id task_id cpus last_heartbeat state")


class HpcState:
    Provisioning, Running, Closed = range(3)
