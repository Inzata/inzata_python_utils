import requests
import re
import datetime
import time
import json

class SDCClient:

   def __init__(self, hostname, port, username, password, retriesAreOK=False):
      self.hostname = hostname
      self.port = port
      self.username = username
      self.password = password
      self.report = dict()
      self.retriesAreOK = retriesAreOK
      self.login()

   def login(self):
      self.session = requests.Session()

      loginUrl = "http://" + self.hostname + ":" + str(self.port) + "/j_security_check"
      credentials = {
         'j_username': self.username,
         'j_password': self.password
      }

      self.session.post(loginUrl, data=credentials)

   def findPipelinesById(self, ids):
      endpointUrl = "http://" + self.hostname + ":" + str(self.port) + "/rest/v1/pipelines"
      jsonPipelineList = self.session.get(endpointUrl).json()
      matchingPipelines = list()

      for pipeline in jsonPipelineList:
         pipID = pipeline['pipelineId']

         if pipID in ids:
            matchingPipelines.append(pipID)

      return matchingPipelines

   def findPipelinesByPrefix(self, pfxs):
      endpointUrl = "http://" + self.hostname + ":" + str(self.port) + "/rest/v1/pipelines"
      jsonPipelineList = self.session.get(endpointUrl).json()
      matchingPipelines = list()

      for pipeline in jsonPipelineList:
         pipID = pipeline['pipelineId']
         pipTitle = pipeline['title']

         for pfx in pfxs:
            if re.search("^" + pfx, pipTitle):
               matchingPipelines.append(pipID)

      return matchingPipelines

   def startPipelines(self, ids, **kwargs):
      self.report = dict()
      urlPrefix = "http://" + self.hostname + ":" + str(self.port) + "/rest/v1/pipeline/"

      for pipId in ids:
         startUrl = urlPrefix + pipId + "/start"
         response = self.session.post(startUrl, headers={ 'X-Requested-By': 'sdc'}, json=kwargs)
         status = response.json()['status']
         status_code = response.status_code
         if (status_code == 200) & (status == 'STARTING'):
            self.report[pipId] = {
               "started": True,
               "startedAt": datetime.datetime.now().replace(microsecond=0)
            }
         else:
            self.report[pipId] = {
               "started": False,
               "error": True,
               "errorDesc": "Failed to start",
               "startedAt": datetime.datetime.now().replace(microsecond=0),
               "statusCode": status_code,
               "lastStatus": "START_FAILED"
            }

   def awaitCompletion(self, ids):
      runningPipelines = list()

      for pipId in ids:
         if self.report[pipId]['started']:
            runningPipelines.append(pipId)

      urlPrefix = "http://" + self.hostname + ":" + str(self.port) + "/rest/v1/pipeline/"

      stillRunning = True if bool(len(runningPipelines)) else False

      while stillRunning:
         for pipId in runningPipelines:

            status = self.session.get(urlPrefix + pipId + "/status").json()['status']
            if status == 'FINISHED':
               self.report[pipId]['error'] = False
               self.report[pipId]['finishedAt'] = datetime.datetime.now().replace(microsecond=0)
               self.report[pipId]['lastStatus'] = status
               runningPipelines.remove(pipId)
               self.report[pipId]['metrics'] = self.metrics(pipId)
            elif self.retriesAreOK and status.startswith("RETRY"):
               print("Pipeline " + pipId + " is restarting")
            elif not ((status == 'STARTING') or (status == 'RUNNING') or (status == 'FINISHING')):
               self.report[pipId]['error'] = True
               self.report[pipId]['errorDesc'] = 'error during run'
               self.report[pipId]['finishedAt'] = datetime.datetime.now().replace(microsecond=0)
               self.report[pipId]['lastStatus'] = status
               runningPipelines.remove(pipId)

         stillRunning = True if bool(len(runningPipelines)) else False
         time.sleep(1)

   def metrics(self, pipId):

      retVal = dict()

      historyUrl = "http://" + self.hostname + ":" + str(self.port) + "/rest/v1/pipeline/"
      historyUrl = historyUrl + pipId + "/history"
      history = self.session.get(historyUrl).json()

      lastTime = 0

      for h in history:
         if h['metrics'] is None: continue
         mtrcs =json.loads(h['metrics'])
         timeStamp = mtrcs['gauges']['RuntimeStatsGauge.gauge']['value']['timeOfLastReceivedRecord']
         if timeStamp > lastTime:
            lastTime = timeStamp
            retVal['errors'] = mtrcs['counters']['pipeline.batchErrorRecords.counter']['count']
            retVal['recordsIn'] = mtrcs['counters']['pipeline.batchInputRecords.counter']['count']
            retVal['recordsOut'] = mtrcs['counters']['pipeline.batchOutputRecords.counter']['count']

      return retVal

   def execPipelinesById(self, ids, **kwargs):
      matchingIds = self.findPipelinesById(ids)

      if (len(matchingIds) < 1):
         raise Exception("No pipelines found with the matching IDs")

      self.startPipelines(matchingIds, **kwargs)
      self.awaitCompletion(matchingIds)

      return self.report

   def execPipelinesByPfx(self, pfxs, **kwargs):
      matchingIds = self.findPipelinesByPrefix(pfxs)

      if (len(matchingIds) < 1):
         raise Exception("No pipelines found with the matching prefixes")

      self.startPipelines(matchingIds, **kwargs)
      self.awaitCompletion(matchingIds)

      return self.report