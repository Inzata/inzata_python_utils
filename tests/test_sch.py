
import unittest
from unittest import mock

from inzata_python_utils.sch import SCHClient

#https://docs.python-guide.org/writing/tests/

class FakeCommit:
   def __init__(self, commitId, commitTime, draft):
      self.draft = draft
      self.commitTime = commitTime
      self.commitId = commitId

class FakePipeline:
   def __init__(self, pipId, pipName):
      self.pipelineId = pipId
      self.name = pipName
      self.commits = []

class MockResponse:
   def __init__(self, json_data, status_code):
      self.json_data = json_data
      self.status_code = status_code

   def json(self):
      return self.json_data

class FakeResponse:
   def __init__(self, json_data, status_code):
      self.response = MockResponse(json_data, status_code)

class FakeJob:
   def __init__(self, jobName):
      self.job_id = 'job'
      self.job_name = jobName
      self._control_hub = None

   def refresh(self):
      self.refreshed = True

class FakeApiClient:
   def __init__(self):
      self.clid = 1

   def get_job_metrics(self, jobId):
      return None


class TestSCHClient(unittest.TestCase):

   def setUp(self):
      super().setUp()

      self.tokenid = 'mytoken'
      self.tokenvalue = 'secrettoken'

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testAuthenticates(self, MockControlHub):

      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      MockControlHub.assert_called()

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testFindsTwoPipelinesById(self, MockControlHub):
      
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      commit = FakeCommit('c1', 1, False)

      client.sch.pipelines = [FakePipeline('aa', 'My A pipeline'),
                              FakePipeline('ab', 'My B pipeline'),
                              FakePipeline('ax', 'My irrelevant pipeline')
                              ]

      for p in client.sch.pipelines:
         p.commits.append(commit)

      pipelines = client.findPipelinesById(['aa', 'ab', 'ac'])
      expectedPips = {
         'aa': {'commit': 'c1', 'id': 'aa', 'name': 'My A pipeline', 'tag': None},
         'ab': {'commit': 'c1', 'id': 'ab', 'name': 'My B pipeline', 'tag': None}
      }
      self.assertDictEqual(expectedPips, pipelines)

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testFindsFourPipelinesByPrefix(self, MockControlHub):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      commit = FakeCommit('c1', 1, False)

      client.sch.pipelines = [FakePipeline('iaaaa', 'aaaa'),
                              FakePipeline('iaa', 'aa'),
                              FakePipeline('iacaa', 'acaa'),
                              FakePipeline('iiabaa', 'abaa'),
                              FakePipeline('iab', 'ab')
                              ]
      
      for p in client.sch.pipelines:
         p.commits.append(commit)

      pipelines = client.findPipelinesByPrefix(['ab', 'aa'])
      expectedPips = {
         'iaaaa': {'commit': 'c1', 'id': 'iaaaa', 'name': 'aaaa', 'tag': None},
         'iaa': {'commit': 'c1', 'id': 'iaa', 'name': 'aa', 'tag': None},
         'iiabaa': {'commit': 'c1', 'id': 'iiabaa', 'name': 'abaa', 'tag': None},
         'iab': {'commit': 'c1', 'id': 'iab', 'name': 'ab', 'tag': None}
      }
      self.assertDictEqual(expectedPips, pipelines)

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testRunsTwoPipelinesWoParams(self, MockControlHub):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      pipMeta = {
         'ab': {'commit': 'c1', 'id': 'ab', 'name': 'aaaa', 'tag': None},
         'aa': {'commit': 'c1', 'id': 'aa', 'name': 'aa', 'tag': None}
      }

      pipelines = client.startPipelines(pipMeta, None)

      self.assertEqual(len(client.sch.start_job.call_args_list), 2)
      client.sch.start_job.assert_called_with(pipMeta['aa']['job'], wait=False)

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testRunsTwoPipelinesWithParams(self, MockControlHub):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      pipMeta = {
         'ab': {'commit': 'c1', 'id': 'ab', 'name': 'aaaa', 'tag': None},
         'aa': {'commit': 'c1', 'id': 'aa', 'name': 'aa', 'tag': None}
      }

      pipelines = client.startPipelines(pipMeta, None, FROM_DATE="2020-01-01")

      self.assertEqual(len(client.sch.start_job.call_args_list), 2)
      client.sch.start_job.assert_called_with(pipMeta['aa']['job'], wait=False, FROM_DATE='2020-01-01')

      self.assertTrue(client.report['ab']['started'])
      self.assertTrue(client.report['aa']['started'])

   @mock.patch('time.sleep', return_value = 1)
   @mock.patch.object(SCHClient, 'metrics', return_value = {})
   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testWaitsForCompletion(self, MockControlHub, MockMetrics, MockSleep):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      pipMeta = {
         'aa': {'commit': 'c1', 'id': 'aa', 'name': 'aaaa', 'tag': None, 'job': FakeJob('job1')},
         'bb': {'commit': 'c1', 'id': 'bb', 'name': 'aa', 'tag': None, 'job': FakeJob('job2')}
      }

      client.sch.get_current_job_status.side_effect = [
         FakeResponse({"id": "aa", "status": "ACTIVATING"}, 200),
         FakeResponse({"id":"bb", "status": "ACTIVATING"}, 200),
         FakeResponse({"id": "aa", "status": "ACTIVE"}, 200),
         FakeResponse({"id":"bb", "status": "DEACTIVATING"}, 200),
         FakeResponse({"id": "aa", "status": "DEACTIVATING"}, 200),
         FakeResponse({"id":"bb", "status": "INACTIVE"}, 200),
         FakeResponse({"id": "aa", "status": "INACTIVE"}, 200)
      ]

      client.report = dict()

      client.report['aa'] = {"started":True}
      client.report['bb'] = {"started":True}

      client.awaitCompletion(pipMeta)

      self.assertEqual(len(client.sch.get_current_job_status.call_args_list), 7)

   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testPicksCorrectMetrics(self, MockControlHub):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      job = FakeJob('job1')
      resp = FakeResponse(
         {'counters':
            {
               'pipeline.batchErrorRecords.counter': {'count':0},
               'pipeline.batchInputRecords.counter': {'count':0},
               'pipeline.batchOutputRecords.counter': {'count':0}
            }
         },
         200
      )

      client.sch.api_client = FakeApiClient()

      client.sch.api_client.get_job_metrics = mock.Mock(return_value=resp)

      job._control_hub = client.sch

      report = client.metrics(job)
      client.sch.api_client.get_job_metrics.assert_called_once()

   @mock.patch.object(SCHClient, 'awaitCompletion', return_value = {})
   @mock.patch.object(SCHClient, 'startPipelines', return_value = {})
   @mock.patch.object(SCHClient, 'findPipelinesById', return_value = ['aa'])
   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testPushesDownIdsAndKwargs(self, MockSch, MockFindPips, MockStartPips, MockAwaitCompl):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      client.execPipelinesById(['aa'], None, None, LOADED_AT="2020-08-01")

      MockStartPips.assert_called_with(['aa'], None, **{'LOADED_AT': '2020-08-01'})

   @mock.patch.object(SCHClient, 'awaitCompletion', return_value = {})
   @mock.patch.object(SCHClient, 'startPipelines', return_value = {})
   @mock.patch.object(SCHClient, 'findPipelinesByPrefix', return_value = ['aa'])
   @mock.patch('inzata_python_utils.sch.ControlHub', autospec=True)
   def testPushesDownPfxsAndKwargs(self, MockSch, MockFindPips, MockStartPips, MockAwaitCompl):
      client = SCHClient(
         self.tokenid,
         self.tokenvalue
      )

      client.execPipelinesByPfx(['aa'], None, None, LOADED_AT="2020-08-01")

      MockStartPips.assert_called_with(['aa'], None, **{'LOADED_AT': '2020-08-01'})

      
