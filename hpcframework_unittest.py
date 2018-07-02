import json
import unittest

from mesoshttp.offers import Offer
from mock import patch, MagicMock, call

import hpcframework


def create_mock_mesos_offer_aux(cpus, max_cores, is_windows, hostname):
    json_offer = '''
    {{
        "hostname": "{}",
        "attributes": [
    '''.format(hostname)
    if (is_windows):
        json_offer += '''
        {
            "text": {
                "value": "windows_server"
            },
            "type": "TEXT",
            "name": "os"
        },
        '''
    json_offer += '''
    {{
            "scalar": {{
                "value": {}
            }},
            "type": "SCALAR",
            "name": "cores"
        }}
    ],
    "resources": [
        {{
            "type": "SCALAR",
            "allocation_info": {{
                "role": "*"
            }},
            "role": "*",
            "name": "cpus",
            "scalar": {{
                "value": {}
            }}
        }}
    ]
}}
    '''.format(max_cores, cpus)
    return json.loads(json_offer)


def create_mock_mesos_offer(cpus, max_cores, is_windows, hostname):
    return Offer("uri", "fid", "sid", create_mock_mesos_offer_aux(cpus, max_cores, is_windows, hostname))


class HpcFrameworkUnitTest(unittest.TestCase):
    def setUp(self):
        self.hpcpackFramework = hpcframework.HpcpackFramwork()

    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_accpet_offer(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=1)
        offer = create_mock_mesos_offer(4.0, 4.0, True, "host1")
        offers = [offer]
        self.hpcpackFramework.offer_received(offers)
        mock_accept_offer.assert_called_with(offer)
        mock_decline_offer.assert_not_called()

    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_no_need_to_grow(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=0)
        offer = create_mock_mesos_offer(4.0, 4.0, True, "host1")
        offers = [offer]
        self.hpcpackFramework.offer_received(offers)
        mock_accept_offer.assert_not_called()
        mock_decline_offer.assert_called_with(offer)

    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_accept_partial_offer(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=2)
        offer1 = create_mock_mesos_offer(1.0, 1.0, True, "host1")
        offer2 = create_mock_mesos_offer(1.0, 1.0, True, "host2")
        offer3 = create_mock_mesos_offer(1.0, 1.0, True, "host3")
        offers = [offer1, offer2, offer3]
        self.hpcpackFramework.offer_received(offers)
        calls = [call(offer1), call(offer2)]
        mock_accept_offer.assert_has_calls(calls)
        mock_decline_offer.assert_called_with(offer3)

    @patch('hpc_cluster_manager.HpcClusterManager.get_cores_in_provisioning')
    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_accept_offer_with_provisioning(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer,
                                            mock_get_cores_in_provisioning):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=5)
        mock_get_cores_in_provisioning.return_value = 1
        offer1 = create_mock_mesos_offer(1.0, 1.0, True, "host1")
        offer2 = create_mock_mesos_offer(1.0, 1.0, True, "host2")
        offer3 = create_mock_mesos_offer(1.0, 1.0, True, "host3")
        offers = [offer1, offer2, offer3]
        self.hpcpackFramework.offer_received(offers)
        calls = [call(offer1), call(offer2), call(offer3)]
        mock_accept_offer.assert_has_calls(calls)
        mock_decline_offer.assert_not_called()

    @patch('hpc_cluster_manager.HpcClusterManager.get_cores_in_provisioning')
    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_accept_partial_offer_with_provisioning(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer,
                                                    mock_get_cores_in_provisioning):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=2)
        mock_get_cores_in_provisioning.return_value = 1
        offer1 = create_mock_mesos_offer(1.0, 1.0, True, "host1")
        offer2 = create_mock_mesos_offer(1.0, 1.0, True, "host2")
        offer3 = create_mock_mesos_offer(1.0, 1.0, True, "host3")
        offers = [offer1, offer2, offer3]
        self.hpcpackFramework.offer_received(offers)
        calls = [call(offer2), call(offer3)]
        mock_accept_offer.assert_called_with(offer1)
        mock_decline_offer.assert_has_calls(calls)

    @patch('hpc_cluster_manager.HpcClusterManager.check_fqdn_collision')
    @patch('hpc_cluster_manager.HpcClusterManager.get_cores_in_provisioning')
    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_declient_offer_on_fqdn_collision(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer,
                                              mock_get_cores_in_provisioning, mock_check_fqdn_collision):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=2)
        mock_get_cores_in_provisioning.return_value = 0
        mock_check_fqdn_collision.return_value = True
        offer1 = create_mock_mesos_offer(1.0, 1.0, True, "host1")
        offers = [offer1]
        self.hpcpackFramework.offer_received(offers)
        mock_accept_offer.assert_not_called()
        mock_decline_offer.assert_called_with(offer1)

    @patch('hpcframework.HpcpackFramwork.decline_offer')
    @patch('hpcframework.HpcpackFramwork.accept_offer')
    @patch('restclient.HpcRestClient.get_grow_decision')
    def test_decline_non_dedicated_offer(self, mock_get_grow_decision, mock_accept_offer, mock_decline_offer):
        mock_get_grow_decision.return_value = MagicMock(cores_to_grow=1)
        offer = create_mock_mesos_offer(4.0, 5.0, True, "host1")
        offers = [offer]
        self.hpcpackFramework.offer_received(offers)
        mock_accept_offer.assert_not_called()
        mock_decline_offer.assert_called_with(offer)


if __name__ == '__main__':
    unittest.main()
