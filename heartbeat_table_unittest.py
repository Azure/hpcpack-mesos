import datetime
import unittest

from heartbeat_table import HeartBeatTable, HpcState

HOST1HOSTNAME = "host1hostname"
HOST1FQDN = "host1hostname.fqdn.com"
HOST1FQDN2 = "host1hostname.fqdn2.com"
HOST1AGENTID = "host1agentid"
HOST1TASKID1 = "host1taskid1"
HOST1TASKID2 = "host1taskid2"

HOST2HOSTNAME = "host2hostname"
HOST2FQDN = "host2hostname.fqdn.com"
HOST2AGENTID = "host2agentid"
HOST2TASKID1 = "host2taskid1"

ZERODELTA = datetime.timedelta(0)


class UTCtzinfo(datetime.tzinfo):
    def utcoffset(self, dt):
        return ZERODELTA

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERODELTA


utc = UTCtzinfo()
UTCNOW = datetime.datetime(2018, 1, 1, 12, 0, 0, 0, tzinfo=utc)
TENMINUTES = datetime.timedelta(minutes=10)
ONESEC = datetime.timedelta(seconds=1)


class HeartbeatTableUnitTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_fqdn_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        self.assertEquals(agent_id, HOST1AGENTID)

    def test_hostname_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        self.assertEquals(agent_id, HOST1AGENTID)

    def test_duplicated_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 2)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID2, 3)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID2)

    def test_same_hostname_add_slaveinfo(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 2)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        heartbeat_table.add_slaveinfo(HOST1FQDN2, HOST1AGENTID, HOST1TASKID2, 3)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)

    def test_host_state_change(self):
        heartbeat_table = HeartBeatTable()
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Unknown)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Provisioning)
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Running)
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Running)
        heartbeat_table.on_slave_close(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Closed)
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Closed)

    def test_check_timeout(self):
        heartbeat_table = HeartBeatTable(TENMINUTES, TENMINUTES)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        # Provisioning state
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES)
        self.assertEqual(provision_timeout_list[0].hostname, HOST1HOSTNAME.upper())
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        # Running state
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME, UTCNOW)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertEqual(running_list[0].hostname, HOST1HOSTNAME.upper())
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES)
        self.assertFalse(provision_timeout_list)
        self.assertEqual(heartbeat_timeout_list[0].hostname, HOST1HOSTNAME.upper())
        self.assertFalse(running_list)
        # Close state
        heartbeat_table.on_slave_close(HOST1HOSTNAME)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table.check_timeout(UTCNOW + TENMINUTES)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)

    def test_get_cores_in_provisioning(self):
        heartbeat_table = HeartBeatTable()
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 0)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 1)
        heartbeat_table.add_slaveinfo(HOST2FQDN, HOST2AGENTID, HOST2TASKID1, 2)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 3)
        heartbeat_table.on_slave_heartbeat(HOST2HOSTNAME)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 1)
        heartbeat_table.on_slave_heartbeat(HOST2HOSTNAME)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 1)
        heartbeat_table.on_slave_close(HOST1HOSTNAME)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 0)

    def test_check_fqdn_collision(self):
        heartbeat_table = HeartBeatTable()
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertTrue(heartbeat_table.check_fqdn_collision(HOST1FQDN2))
        self.assertFalse(heartbeat_table.check_fqdn_collision(HOST1FQDN))


if __name__ == '__main__':
    unittest.main()
