import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from urllib import request

ALIGN_CPUS = int(os.getenv("ALIGN_CPUS", "24"))
MIN_PROCESS_MEM = 20
STORAGE_ROOT = os.getenv("STORAGE_ROOT", "./static")
WES_BASE = os.getenv("WES_BASE")
SONG_URL = os.getenv("SONG_URL")
SCORE_URL = os.getenv("SCORE_URL")
INTERMEDIATE_SONG_URL = os.getenv("INTERMEDIATE_SONG_URL")
ICGC_SCORE_URL = os.getenv("ICGC_SCORE_URL")
API_TOKEN = os.getenv("SONG_API_TOKEN")
ICGC_SCORE_TOKEN = os.getenv("ICGC_SCORE_API_TOKEN")

def getWesRuns(wesIds):
    loop = asyncio.get_event_loop()
    coroutines = [getWesRun(wesId) for wesId in wesIds]
    return loop.run_until_complete(asyncio.gather(*coroutines))


def getRunsAsDataframe(wesIds):
    runs = getWesRuns(wesIds)
    # TODO remove this step in prod, it is here because some runs do not have analysis id
    runs = [run for run in runs if run]
    return pd.DataFrame(runs)


async def getWesRun(wesId):
    async with aiohttp.ClientSession() as session:
        async with session.get("{}{}".format(WES_BASE, wesId.strip())) as response:
            # TODO remove this try.except in prod, it is here because some runs do not have analysis id
            try:
                data = await response.json()
                return {
                    "analysis_id": data["request"]["workflow_params"]["analysis_id"],
                    "run_id": data["run_id"],
                    "state": data["state"],
                    "params": data["request"]["workflow_params"],
                    "start": data["run_log"]["start_time"],
                    "end": data["run_log"]["end_time"],
                    "duration": data["run_log"]["duration"],
                    "tasks": list(filter(None, map(processTasks, data["task_logs"])))
                }
            except Exception:
                pass


def getWesRunIds():
    data = requests.get(WES_BASE).json()
    return [run["run_id"] for run in data["runs"]]


def startWesRuns(paramsList):
    loop = asyncio.get_event_loop()
    coroutines = [startVariableParamsRun(params) for params in paramsList]
    return loop.run_until_complete(asyncio.gather(*coroutines))


async def startVariableParamsRun(params, semaphore=asyncio.Semaphore(5)):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            print("Starting new job for analysisId: ", params["analysisId"])

            cpus = ALIGN_CPUS
            mem = max((cpus * 3) + 2, MIN_PROCESS_MEM)
            nfs = params["nfs"]
            analysisId = params["analysisId"]
            studyId = params["studyId"]

            # TODO: Check params with Junjun
            payload = {
                "workflow_url": "icgc-argo/dna-seq-processing-wfs",
                "workflow_params": {
                    "study_id": studyId,
                    "analysis_id": analysisId,
                    "song_url": SONG_URL,
                    "score_url": SCORE_URL,
                    "api_token": API_TOKEN,
                    "ref_genome_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(nfs),
                    "cpus": 2,
                    "mem": 4,
                    "download": {
                        "song_cpus": 2,
                        "song_mem": 2,
                        "score_cpus": 8,
                        "score_mem": 18
                    },
                    "seqDataToLaneBam": {
                        "cpus": cpus,
                        "mem": mem
                    },
                    "bwaMemAligner": {
                        "cpus": cpus,
                        "mem": mem
                    },
                    "bamMergeSortMarkdup": {
                         "cpus": 4,
                        "mem": 18
                    },
                    "payloadGenDnaAlignment": {
                        "cpus": 2,
                        "mem": 4
                    },
                    "readGroupUBamQC": {
                        "cpus": 3,
                        "mem": 6
                    },
                    "alignedSeqQC": {
                        "cpus": 4,
                        "mem": 10
                    },
                    "gatkCollectOxogMetrics": {
                        "cpus": 3,
                        "mem": 6
                    },
                    "payloadGenDnaSeqQc": {
                        "cpus": 2,
                        "mem": 2
                    },
                    "uploadAlignment": {
                        "song_cpus": 2,
                        "song_mem": 2,
                        "score_cpus": 8,
                        "score_mem": 18
                    },
                    "uploadQc": {
                        "song_cpus": 2,
                        "song_mem": 2,
                        "score_cpus": 2,
                        "score_mem": 4
                    },
                    "cleanup": True
                },
                "workflow_engine_params": {
                    "work_dir": "/{}/work".format(nfs),
                    "revision": "master"
                }
            }

            if INTERMEDIATE_SONG_URL:
                payload["workflow_params"]["download"]["song_url"] = INTERMEDIATE_SONG_URL
                payload["workflow_params"]["download"]["score_url"] = ICGC_SCORE_URL
                payload["workflow_params"]["download"]["score_api_token"] = ICGC_SCORE_TOKEN

            async with session.post(WES_BASE, json=payload) as response:
                data = await response.json()
                # return format for easy write into sheets as column
                return [data["run_id"]]


def processTasks(task):
    if task["state"] == "COMPLETE" and task["exit_code"] != "0":
        return {
            "process": task["process"],
            "tag": task["tag"],
            "cpus": task["cpus"],
            "memory": abs(task["memory"]),
            "duration": task["duration"],
            "realtime": task["realtime"]
        }


def extractTaskInfo(id, task):
    return [id, task["tag"], task["realtime"], task["cpus"]]


def extractRunInfo(run):
    return [extractTaskInfo(run["runId"], task) for task in run["tasks"] if task["process"] == "align"]
