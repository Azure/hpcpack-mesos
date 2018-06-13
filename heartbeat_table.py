import threading
from collections import namedtuple
from datetime import datetime, timedelta

import logging_aux


class HeartBeatTable(object):

    CHECK_CONFIGURING_NODES_INTERVAL = 5 # in seconds

    def __init__(self, provisioning_timeout=timedelta(minutes=15), heartbeat_timeout=timedelta(minutes=3)):
        self._table = {}
        self.logger = logging_aux.init_logger_aux("hpcframework.heartbeat", "hpcframework.heartbeat.log")
        self.on_node_configuring = []
        self._table_lock = threading.Lock()
        self._provisioning_timeout = provisioning_timeout
        self._heartbeat_timeout = heartbeat_timeout

    def __get_hostname_from_fqdn(self, fqdn):
        return fqdn.split('.')[0]

    def subscribe_node_configuring(self, callback):
        self.on_node_configuring.append(callback)

    def add_slaveinfo(self, fqdn, agent_id, task_id, cpus, last_heartbeat=datetime.utcnow()):
        u_fqdn = fqdn.upper()
        hostname = self.__get_hostname_from_fqdn(u_fqdn)
        if hostname in self._table:
            if self._table[hostname].fqdn != u_fqdn:
                self.logger.error("Duplicated hostname {} detected. Existing fqdn: {}, new fqdn {}. Ignore new heartbeat entry.".format(
                    hostname, self._table[hostname].fqdn, u_fqdn))
                return
            elif self._table[hostname].state != HpcState.Closed:
                self.logger.warn("Heart beat entry of {} existed. old value: {}.".format(hostname, str(self._table[hostname])))
        slaveinfo = SlaveInfo(hostname, u_fqdn, agent_id, task_id, cpus, last_heartbeat, HpcState.Provisioning)
        self._table[hostname] = slaveinfo
        self.logger.info("Heart beat entry added: {}".format(str(slaveinfo)))

    def on_slave_heartbeat(self, hostname, now=datetime.utcnow()):
        u_hostname = hostname.upper()
        if u_hostname in self._table:
            self._table[u_hostname] = self._table[u_hostname]._replace(last_heartbeat=now)
            self.logger.info("Heatbeat from host {}".format(u_hostname))
            if self._table[u_hostname].state == HpcState.Provisioning:
                with self._table_lock:
                    if self._table[u_hostname].state == HpcState.Provisioning:
                        self._table[u_hostname] = self._table[u_hostname]._replace(state=HpcState.Configuring)
                        self.logger.info("Configuring Host {}".format(u_hostname))
        else:
            self.logger.error("Host {} is not recognized. Heartbeat ignored.".format(u_hostname))
            self.logger.error("_table {} ".format(self._table))

    def on_slave_close(self, hostname):
        u_hostname = hostname.upper()
        if u_hostname in self._table:
            self._table[u_hostname] = self._table[u_hostname]._replace(state=HpcState.Closed)
            self.logger.info("Host {} closed".format(u_hostname))
        else:
            self.logger.error("Host {} is not recognized. Close event ignored.".format(u_hostname))

    def get_task_info(self, hostname):
        u_hostname = hostname.upper()
        if u_hostname in self._table:
            entry = self._table[u_hostname]
            return (entry.task_id, entry.agent_id)
        else:
            self.logger.error("Host {} is not recognized. Failed to get task info.".format(u_hostname))
            return ("", "")

    def get_host_state(self, hostname):
        u_hostname = hostname.upper()
        if u_hostname in self._table:
            entry = self._table[u_hostname]
            return entry.state
        else:
            self.logger.error("Host {} is not recognized. Failed to get host state.".format(u_hostname))
            return HpcState.Unknown

    def __exec_callback(self, callbacks):
        for callback in callbacks:
            try:
                self.logger.debug('Callback %s on %s' % (callback.__name__))
                callback()
            except Exception as e:
                self.logger.exception('Error in %s callback: %s' % (callback.__name__, str(e)))

    def check_fqdn_collision(self, fqdn):
        u_fqdn = fqdn.upper()
        hostname = self.__get_hostname_from_fqdn(u_fqdn)
        if hostname in self._table:
            if self._table[hostname].fqdn != u_fqdn:
                return True
        return False

    def check_timeout(self, now=datetime.utcnow()):
        provision_timeout_list = []
        heartbeat_timeout_list = []
        running_list = []
        for host in dict(self._table).itervalues():
            if host.state == HpcState.Provisioning and now - host.last_heartbeat >= self._provisioning_timeout:
                self.logger.warn("Provisioning timeout: {}".format(str(host)))
                provision_timeout_list.append(host)
            elif host.state == HpcState.Running:
                if now - host.last_heartbeat >= self._heartbeat_timeout:
                    self.logger.warn("Heartbeat lost: {}".format(str(host)))
                    heartbeat_timeout_list.append(host)
                else:
                    running_list.append(host)
        return (provision_timeout_list, heartbeat_timeout_list, running_list)
    
    def get_cores_in_provisioning(self):
        cores = 0.0
        for host in dict(self._table).itervalues():
            if host.state == HpcState.Provisioning:
                cores += host.cpus
        self.logger.info("Cores in provisioning: {}".format(cores))
        return cores

    def _configure_compute_nodes(self):
        configuring_node_names = []
        configured_node_names = []
        for host in dict(self._table).itervalues():
            if host.state == HpcState.Configuring:
                configuring_node_names.append(host.hostname)
                self.logger.info("Nodes in configuring: {}".format(configuring_node_names))        
        
        if configuring_node_names:
            for callback in self.on_node_configuring:
                configured_node_names = callback(configuring_node_names)
                self.logger.info("Nodes configured: {}".format(configured_node_names))    

        if configured_node_names:
            for node_name in configured_node_names:
                self._table[node_name.upper()]._replace(state=HpcState.Running)

    def start_configure_compute_nodes_timer(self):
        self._configure_compute_nodes()
        timer = threading.Timer(self.CHECK_CONFIGURING_NODES_INTERVAL, self.start_configure_compute_nodes_timer)
        timer.daemon = True
        timer.start()

    def start(self):
        self.start_configure_compute_nodes_timer()



SlaveInfo = namedtuple("SlaveInfo", "hostname fqdn agent_id task_id cpus last_heartbeat state")


class HpcState:
    Unknown, Provisioning, Configuring, Running, Closing, Closed = range(6)
