
import unittest
from unittest import mock

import requests

from inzata_python_utils.sdc import SDCClient

#https://docs.python-guide.org/writing/tests/

class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

class TestSDCClient(unittest.TestCase):

   def setUp(self):
      super().setUp()

      self.hostname = 'hostname'
      self.port = 1111
      self.username = 'user1'
      self.password = 'pwd'

   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testAuthenticates(self, MockSession):
      session = MockSession()
      session.post.return_value = requests.models.Response()

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      session.post.assert_called()

   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testFindsTwoPipelinesById(self, MockSession):
      session = MockSession()

      session.post.side_effect = [
         MockResponse({"access_token": "aaa"}, 200)
      ]

      session.get.side_effect = [
         MockResponse(
            [
               {"pipelineId":"aaaa", "title":"aaaa"},
               {"pipelineId":"aa",   "title":"aa"},
               {"pipelineId":"acaa", "title":"acaa"},
               {"pipelineId":"abaa", "title":"abaa"},
               {"pipelineId":"ab",   "title":"ab"}
            ], 
            200)
      ]

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      pipelines = client.findPipelinesById(['aa', 'ab', 'ac'])
      self.assertEqual(sorted(pipelines), sorted(['aa', 'ab']))

   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testFindsFourPipelinesByPrefix(self, MockSession):
      session = MockSession()

      session.post.side_effect = [
         MockResponse({"access_token": "aaa"}, 200)
      ]

      session.get.side_effect = [
         MockResponse(
            [
               {"pipelineId":"iaaaa",  "title":"aaaa"},
               {"pipelineId":"iaa",    "title":"aa"},
               {"pipelineId":"iacaa",  "title":"acaa"},
               {"pipelineId":"iiabaa", "title":"abaa"},
               {"pipelineId":"iab",    "title":"ab"}
            ], 
            200)
      ]

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      pipelines = client.findPipelinesByPrefix(['ab', 'aa'])
      self.assertEqual(sorted(pipelines), sorted(['iaaaa', 'iaa', 'iiabaa', 'iab']))


   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testRunsTwoPipelinesWoParams(self, MockSession):
      session = MockSession()

      session.post.side_effect = [
         MockResponse({"access_token": "aaa"}, 200),
         MockResponse({"status":"STARTING"}, 200),
         MockResponse({"status":"START_ERROR"}, 500)
      ]

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      pipelines = client.startPipelines(['ab', 'aa'])

      self.assertEqual(len(session.post.call_args_list), 3)

      expectedUrl = 'http://' + self.hostname + ':' + str(self.port) + '/rest/v1/pipeline/aa/start'
      session.post.assert_called_with(expectedUrl, headers={'X-Requested-By': 'sdc'}, json={})

   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testRunsTwoPipelinesWithParams(self, MockSession):
      session = MockSession()

      session.post.side_effect = [
         MockResponse({"access_token": "aaa"}, 200),
         MockResponse({"status":"STARTING"}, 200),
         MockResponse({"status":"START_ERROR"}, 500)
      ]

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      pipelines = client.startPipelines(['ab', 'aa'], FROM_DATE="2020-01-01")

      self.assertEqual(len(session.post.call_args_list), 3)

      expectedUrl = 'http://' + self.hostname + ':' + str(self.port) + '/rest/v1/pipeline/aa/start'
      session.post.assert_called_with(expectedUrl, headers={'X-Requested-By': 'sdc'}, json={'FROM_DATE': '2020-01-01'})

      self.assertTrue(client.report['ab']['started'])
      self.assertFalse(client.report['aa']['started'])

   @mock.patch('time.sleep', return_value = 1)
   @mock.patch.object(SDCClient, 'metrics', return_value = {})
   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testWaitsForCompletion(self, MockSession, MockMetrics, MockSleep):
      session = MockSession()
      session.post.return_value = requests.models.Response()

      session.get.side_effect = [
         MockResponse({"id": "aa", "status": "RUNNING"}, 200),
         MockResponse({"id":"bb", "status": "RUNNING"}, 200),
         MockResponse({"id": "aa", "status": "RUNNING"}, 200),
         MockResponse({"id":"bb", "status": "RUNNING"}, 200),
         MockResponse({"id": "aa", "status": "FINISHING"}, 200),
         MockResponse({"id":"bb", "status": "FAILED"}, 200),
         MockResponse({"id": "aa", "status": "FINISHED"}, 200)
      ]

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      client.report['aa'] = {"started":True}
      client.report['bb'] = {"started":True}

      client.awaitCompletion(['aa', 'bb'])

      self.assertEqual(len(session.get.call_args_list), 7)
      self.assertFalse(client.report['aa']['error'])
      self.assertTrue(client.report['bb']['error'])
      MockMetrics.assert_called_once()

   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testPicksCorrectMetrics(self, MockSession):
      session = MockSession()
      session.post.return_value = requests.models.Response()

      session.get.return_value = MockResponse(
         [
            {
               "pipelineId": "testpipid",
               "metrics": "{\"gauges\":{\"RuntimeStatsGauge.gauge\":{\"value\":{\"timeOfLastReceivedRecord\":1617887421248}}},\"counters\":{\"pipeline.batchErrorRecords.counter\":{\"count\":1},\"pipeline.batchInputRecords.counter\":{\"count\":2},\"pipeline.batchOutputRecords.counter\":{\"count\":3}}}"
            },
            {
               "pipelineId": "testpipid",
               "metrics": "{\"gauges\":{\"RuntimeStatsGauge.gauge\":{\"value\":{\"timeOfLastReceivedRecord\":1617887421244}}},\"counters\":{\"pipeline.batchErrorRecords.counter\":{\"count\":2},\"pipeline.batchInputRecords.counter\":{\"count\":3},\"pipeline.batchOutputRecords.counter\":{\"count\":4}}}"
            },
         ], 200)

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      report = client.metrics('testpipid')
      self.assertDictEqual({'errors': 1, 'recordsIn': 2, 'recordsOut': 3}, report)
      session.get.assert_called_once()


   @mock.patch.object(SDCClient, 'awaitCompletion', return_value = {})
   @mock.patch.object(SDCClient, 'startPipelines', return_value = {})
   @mock.patch.object(SDCClient, 'findPipelinesById', return_value = ['aa'])
   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testPushesDownIdsAndKwargs(self, MockSession, MockFindPips, MockStartPips, MockAwaitCompl):
      session = MockSession()
      session.post.return_value = requests.models.Response()

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      client.execPipelinesById(['aa'], LOADED_AT="2020-08-01")

      MockStartPips.assert_called_with(['aa'], **{'LOADED_AT': '2020-08-01'})


   @mock.patch.object(SDCClient, 'awaitCompletion', return_value = {})
   @mock.patch.object(SDCClient, 'startPipelines', return_value = {})
   @mock.patch.object(SDCClient, 'findPipelinesByPrefix', return_value = ['aa'])
   @mock.patch('inzata_python_utils.sdc.requests.Session')
   def testPushesDownPfxsAndKwargs(self, MockSession, MockFindPips, MockStartPips, MockAwaitCompl):
      session = MockSession()
      session.post.return_value = requests.models.Response()

      client = SDCClient(
         self.hostname,
         self.port,
         self.username,
         self.password
      )

      client.execPipelinesByPfx(['aa'], LOADED_AT="2020-08-01")

      MockStartPips.assert_called_with(['aa'], **{'LOADED_AT': '2020-08-01'})








