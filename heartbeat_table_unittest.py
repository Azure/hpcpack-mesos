import datetime
import unittest

from mock import patch

from hpc_cluster_manager import HpcClusterManager, HpcState

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

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_fqdn_add_slaveinfo(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        self.assertEquals(agent_id, HOST1AGENTID)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_hostname_add_slaveinfo(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        heartbeat_table.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1)
        (task_id, agent_id) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        self.assertEquals(agent_id, HOST1AGENTID)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_duplicated_add_slaveinfo(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 2)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID2, 3)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID2)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_same_hostname_add_slaveinfo(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 2)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)
        heartbeat_table.add_slaveinfo(HOST1FQDN2, HOST1AGENTID, HOST1TASKID2, 3)
        (task_id, _) = heartbeat_table.get_task_info(HOST1HOSTNAME)
        self.assertEquals(task_id, HOST1TASKID1)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_host_state_change(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Unknown)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Provisioning)
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Configuring)
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME)
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Configuring)
        heartbeat_table._set_nodes_running([HOST1HOSTNAME])
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Running)
        heartbeat_table._set_nodes_draining([HOST1HOSTNAME])
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Draining)
        heartbeat_table._set_nodes_closing([HOST1HOSTNAME])
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Closing)
        heartbeat_table._set_nodes_closed([HOST1HOSTNAME])
        self.assertEquals(heartbeat_table.get_host_state(HOST1HOSTNAME), HpcState.Closed)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_timeout(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc, TENMINUTES, TENMINUTES)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        # Provisioning state
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES)
        self.assertEqual(provision_timeout_list[0].hostname, HOST1HOSTNAME.upper())
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        # Heart beat will lead into Configuring state
        heartbeat_table.on_slave_heartbeat(HOST1HOSTNAME, UTCNOW)
        # TODO: Add and test configuring timeout
        # Running state
        heartbeat_table._set_nodes_running([HOST1HOSTNAME])
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertEqual(running_list[0].hostname, HOST1HOSTNAME.upper())
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES)
        self.assertFalse(provision_timeout_list)
        self.assertEqual(heartbeat_timeout_list[0].hostname, HOST1HOSTNAME.upper())
        self.assertFalse(running_list)
        # Close state
        heartbeat_table._set_nodes_closed([HOST1HOSTNAME])
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)
        (provision_timeout_list, heartbeat_timeout_list, running_list) = heartbeat_table._check_timeout(
            UTCNOW + TENMINUTES)
        self.assertFalse(provision_timeout_list)
        self.assertFalse(heartbeat_timeout_list)
        self.assertFalse(running_list)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_get_cores_in_provisioning(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 0)
        # Add 1 core
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 1)
        # Add 2 cores
        heartbeat_table.add_slaveinfo(HOST2FQDN, HOST2AGENTID, HOST2TASKID1, 2)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 3)
        # Get 2 cores to configuring
        heartbeat_table.on_slave_heartbeat(HOST2HOSTNAME)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 3)
        # Get same 2 cores to configuring
        heartbeat_table.on_slave_heartbeat(HOST2HOSTNAME)
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 3)
        # Get 1 core to running
        heartbeat_table._set_nodes_running([HOST1HOSTNAME])
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 2)
        # Get 1 core to closed
        heartbeat_table._set_nodes_closed([HOST1HOSTNAME])
        self.assertEqual(heartbeat_table.get_cores_in_provisioning(), 2)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_fqdn_collision(self, mock_restc):
        heartbeat_table = HpcClusterManager(mock_restc)
        heartbeat_table.add_slaveinfo(HOST1FQDN, HOST1AGENTID, HOST1TASKID1, 1)
        self.assertTrue(heartbeat_table.check_fqdn_collision(HOST1FQDN2))
        self.assertFalse(heartbeat_table.check_fqdn_collision(HOST1FQDN))

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_node_idle_not_idle(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, idle_timeout=TENMINUTES)
        clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW)
        self.assertFalse(clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW + TENMINUTES - ONESEC))

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_node_idle_idle(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, idle_timeout=TENMINUTES)
        clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW)
        self.assertTrue(clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW + TENMINUTES))

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_node_idle_not_idle_after_remove(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, idle_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1)
        clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW)
        clusmgr._set_nodes_closed([HOST1HOSTNAME])
        self.assertFalse(clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW + TENMINUTES))

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_check_node_idle_idle_after_remove(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, idle_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1)
        clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW)
        clusmgr._set_nodes_closed([HOST1HOSTNAME])
        clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW + TENMINUTES)
        self.assertTrue(clusmgr._check_node_idle_timeout([HOST1HOSTNAME], UTCNOW + TENMINUTES + TENMINUTES))

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_provisioning_time_out_negative(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, provisioning_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        res, _, _ = clusmgr._check_timeout(UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(res)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_provisioning_time_out_positive(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, provisioning_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        res, _, _ = clusmgr._check_timeout(UTCNOW + TENMINUTES)
        self.assertTrue(res)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_heartbeat_time_out_negative(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, heartbeat_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        clusmgr._set_nodes_running([HOST1HOSTNAME])
        _, res, _ = clusmgr._check_timeout(UTCNOW + TENMINUTES - ONESEC)
        self.assertFalse(res)

    @patch("hpc_cluster_manager.HpcRestClient", autospec=True)
    def test_heartbeat_time_out_positive(self, mock_restc):
        clusmgr = HpcClusterManager(mock_restc, heartbeat_timeout=TENMINUTES)
        clusmgr.add_slaveinfo(HOST1HOSTNAME, HOST1AGENTID, HOST1TASKID1, 1, UTCNOW)
        clusmgr._set_nodes_running([HOST1HOSTNAME])
        _, res, _ = clusmgr._check_timeout(UTCNOW + TENMINUTES)
        self.assertTrue(res)


if __name__ == '__main__':
    unittest.main()
