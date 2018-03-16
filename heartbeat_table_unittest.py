import unittest

from heartbeat_table import HeartBeatTable, HpcState

host1hostname = "host1hostname"
host1fqdn = "host1hostname.fqdn.com"
host1fqdn2 = "host1hostname.fqdn2.com"
host1agentid = "host1agentid"
host1taskid1 = "host1taskid1"
host1taskid2 = "host1taskid2"


class HeartbeatTableUnitTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_fqdn_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(host1fqdn, host1agentid, host1taskid1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid1)
        self.assertEquals(agent_id, host1agentid)

    def test_hostname_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(host1hostname, host1agentid, host1taskid1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid1)
        self.assertEquals(agent_id, host1agentid)

    def test_duplicated_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(host1fqdn, host1agentid, host1taskid1, 2)
        (task_id, _) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid1)
        heartbeat_table.add_slaveinfo(host1fqdn, host1agentid, host1taskid2, 3)
        (task_id, _) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid2)

    def test_same_hostname_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(host1fqdn, host1agentid, host1taskid1, 2)
        (task_id, _) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid1)
        heartbeat_table.add_slaveinfo(host1fqdn2, host1agentid, host1taskid2, 3)
        (task_id, _) = heartbeat_table.get_task_info(host1hostname)
        self.assertEquals(task_id, host1taskid1)

    def test_host_state_change(self):
        heartbeat_table = HeartBeatTable()
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Unknown)
        heartbeat_table.add_slaveinfo(host1fqdn, host1agentid, host1taskid1, 1)
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Provisioning)
        heartbeat_table.on_slave_heartbeat(host1hostname)
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Running)
        heartbeat_table.on_slave_heartbeat(host1hostname)
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Running)
        heartbeat_table.on_slave_close(host1hostname)
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Closed)
        heartbeat_table.on_slave_heartbeat(host1hostname)
        self.assertEquals(heartbeat_table.get_host_state(host1hostname), HpcState.Closed)


if __name__ == '__main__':
    unittest.main()
