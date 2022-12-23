from streamsets.sdk import ControlHub
import re
import datetime
import time

class SCHClient:
    def __init__(self, credentialId, token, retriesAreOK=False, defaultTag=None):
        self.credentialId = credentialId
        self.token = token
        self.report = dict()
        self.retriesAreOK = retriesAreOK
        self.defaultTag = defaultTag
        self.login()
        
    def login(self):
        self.sch = ControlHub(credential_id=self.credentialId, token=self.token)
        
    def getLatestPipelineCommit(self, pipeline, tag):
        commitId = None
        tagObj = None
        
        if tag is None:
            latestTime = 0
            
            for c in pipeline.commits:
                if c.draft:
                    continue
                if c.commitTime > latestTime:
                    latestTime = c.commitTime
                    commitId = c.commitId
                    
            if commitId is None:
                raise Exception(f"Failed to find a non-draft commit of the {pipeline.name} pipeline ({pipeline.pipelineId})")
        else:
            for t in pipeline.tags:
                if t.name == tag:
                    tentativeCommitId = t.commitId
                    
                    for c in pipeline.commits:
                        if c.draft:
                            continue
                        if tentativeCommitId == c.commitId:
                            commitId = tentativeCommitId
                            tagObj = t
                            break
                    
                    break
            if commitId is None:
                raise Exception(f"Failed to find a non-draft commit of the {pipeline.name} pipeline ({pipeline.pipelineId}) with the tag of '{tag}'")
                
        return (commitId, tagObj)

        
    def findPipelinesById(self, ids, commitTag=None):
        tag = commitTag
        if tag is None:
            tag = self.defaultTag
            
        matchingPipelines = dict()
        
        for p in self.sch.pipelines:
            pipID = p.pipelineId
            if pipID in ids:
                (commitID, tagObj) = self.getLatestPipelineCommit(p, tag)
                matchingPipelines[pipID] = { 'id': pipID, 'name': p.name, 'commit': commitID, 'tag': tag}
                if tagObj is not None:
                    matchingPipelines[pipID]['tagObj'] = tagObj
                    
                
        return matchingPipelines
    
    def findPipelinesByPrefix(self, pfxs, commitTag=None):
        tag = commitTag
        if tag is None:
            tag = self.defaultTag
            
        matchingPipelines = dict() 
        
        for p in self.sch.pipelines:
            pipID = p.pipelineId
            pipTitle = p.name
            
            for pfx in pfxs:
                if re.search("^" + pfx, pipTitle):
                    (commitID, tagObj) = self.getLatestPipelineCommit(p, tag)
                    matchingPipelines[pipID] = { 'id': pipID, 'name': pipTitle, 'commit': commitID, 'tag': tag}
                    if tagObj is not None:
                        matchingPipelines[pipID]['tagObj'] = tagObj
        
        return matchingPipelines
    
    def findJob(self, jobDesc, tag):
        tagStr = tag
        if tagStr is None:
            tagStr = 'None'
            
        print("Searching for an existing job meeting this criteria:")
        print(f"\tPipeline ID: {jobDesc['id']} ({jobDesc['name']})")
        print(f"\tPipeline Commit: {jobDesc['commit']}")
        print(f"\tSDC tag: {tagStr}")
        
        for job in self.sch.jobs:
            if job.pipeline_id == jobDesc['id']:                
                print(f"'{job.job_name}' ({job.job_id}) has a matching Pipeline-ID")
                if job.commit_id == jobDesc['commit']:
                    print("The pipeline commit ID matches, too")
                    if tag is None:
                        print("The job is good enough. Bingo!")
                        return job
                    else:
                        if tag in job.data_collector_labels:
                            print("The SDC tag matches as well. Bingo!")
                            return job
                        else:
                            print("The SDC tag does not match, though")
                else:
                    print("The pipeline commit ID does not match, though")
                    continue
        print("None of the existing jobs match the criteria")
        return None
        
    def createJob(self, jobDesc, tag):
        jobName = 'Job for ' + jobDesc['name']
        if jobDesc['tag'] is not None:
            jobName += ' (' + jobDesc['tag'] + ')'        
        
        job_builder = self.sch.get_job_builder()
        pipeline = self.sch.pipelines.get(pipeline_id=jobDesc['id'])
        
        pipTag = None
        if ('tagObj' in jobDesc.keys()) and jobDesc['tagObj'] is not None:
            pipTag = jobDesc['tagObj']
        
        job = job_builder.build(jobName, pipeline=pipeline, pipeline_tag=pipTag)
        
        self.sch.add_job(job)
        
        if tag is not None:
            job.data_collector_labels.clear()
            job.data_collector_labels.append(tag)
            self.sch.update_job(job)
            
        self.sch.upgrade_job(job)
        
        return job
    
    def getJob(self, jobDesc, tag):
        job = self.findJob(jobDesc, tag)
        
        if job is not None:
            return job
        
        job = self.createJob(jobDesc, tag)
        
        if job is None:
            tagName = tag
            if tagName is None:
                tagName = 'None'
            raise Exception(f"Failed to create a new job for the {jobDesc['name']} pipeline ({jobDesc['id']}) and the SDC tag {tagName}")
            
        return job
    
    def startPipelines(self, pipMeta, sdcTag, **kwargs):
        self.report = dict()
        
        tag = sdcTag
        if tag is None:
            tag = self.defaultTag
        
        for pipId in pipMeta.keys():
            j = self.getJob(pipMeta[pipId], tag)
            pipMeta[pipId]['job'] = j
            
            try:
                self.sch.start_job(j, wait=False, **kwargs)
                self.report[pipId] = {
                    "started": True,
                    "startedAt": datetime.datetime.now().replace(microsecond=0)
                }
            except Exception as inst:
                self.report[pipId] = {
                    "started": False,
                    "error": True,
                    "errorDesc": "Failed to start",
                    "startedAt": datetime.datetime.now().replace(microsecond=0),
                    "statusCode": type(inst),
                    "lastStatus": type(inst)
                }

    def awaitCompletion(self, pipMeta):        
        runningPipelines = list()
        
        for pipId in pipMeta.keys():
            if self.report[pipId]['started']:
                runningPipelines.append(pipId)
                
        stillRunning = True if bool(len(runningPipelines)) else False
        
        while stillRunning:
            for pipId in runningPipelines:
                jobStatus = self.sch.get_current_job_status(pipMeta[pipId]['job']).response.json()['status']
                
                if 'INACTIVE' in jobStatus:
                    pipMeta[pipId]['job'].refresh()
                    #pipelineStatus = pipMeta[pipId]['job'].pipeline_status.pop().status - test for emptiness
                    self.report[pipId]['error'] = False
                    self.report[pipId]['finishedAt'] = datetime.datetime.now().replace(microsecond=0)
                    self.report[pipId]['lastStatus'] = jobStatus
                    runningPipelines.remove(pipId)
                    print(f"Status of the {pipMeta[pipId]['job'].job_name} is {jobStatus}")
                    #print(f"Status of the corresponding pipeline is {pipelineStatus}")
                    self.report[pipId]['metrics'] = self.metrics(pipMeta[pipId]['job'])
                elif not ((jobStatus == 'ACTIVE') or (jobStatus == 'ACTIVATING') or (jobStatus == 'DEACTIVATING')):
                    pipMeta[pipId]['job'].refresh()
                    self.report[pipId]['error'] = True
                    self.report[pipId]['errorDesc'] = 'error during run'
                    self.report[pipId]['finishedAt'] = datetime.datetime.now().replace(microsecond=0)
                    self.report[pipId]['lastStatus'] = jobStatus
                    print(f"Status of the {pipMeta[pipId]['job'].job_name} is {jobStatus}")
                    runningPipelines.remove(pipId)                 
                    
                    
   #          elif self.retriesAreOK and status.startswith("RETRY"):
   #             print("Pipeline " + pipId + " is restarting")
   #          elif not ((status == 'STARTING') or (status == 'RUNNING') or (status == 'FINISHING')):
   #             self.report[pipId]['error'] = True
   #             self.report[pipId]['errorDesc'] = 'error during run'
   #             self.report[pipId]['finishedAt'] = datetime.datetime.now().replace(microsecond=0)
   #             self.report[pipId]['lastStatus'] = status
   #             runningPipelines.remove(pipId)
            stillRunning = True if bool(len(runningPipelines)) else False
            time.sleep(1)

    def metrics(self, job):
        retVal = dict()
        
        mtrcs = job._control_hub.api_client.get_job_metrics(job.job_id).response.json()
        
        retVal['errors'] = mtrcs['counters']['pipeline.batchErrorRecords.counter']['count']
        retVal['recordsIn'] = mtrcs['counters']['pipeline.batchInputRecords.counter']['count']
        retVal['recordsOut'] = mtrcs['counters']['pipeline.batchOutputRecords.counter']['count']
        
        return retVal
   
    def execPipelinesById(self, ids, commitTag, sdcTag, **kwargs):
        matchingIds = self.findPipelinesById(ids, commitTag)
        
        if (len(matchingIds) < 1):
            raise Exception("No pipelines found with the matching IDs")
            
        self.startPipelines(matchingIds, sdcTag, **kwargs)
        self.awaitCompletion(matchingIds)
        
        return self.report
    
    def execPipelinesByPfx(self, pfxs, commitTag, sdcTag, **kwargs):
        matchingIds = self.findPipelinesByPrefix(pfxs, commitTag)
        
        if (len(matchingIds) < 1):
            raise Exception("No pipelines found with the matching prefixes")
            
        self.startPipelines(matchingIds, sdcTag, **kwargs)
        self.awaitCompletion(matchingIds)
        
        return self.report
