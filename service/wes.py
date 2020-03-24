import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from urllib import request


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
        async with session.get("{}/{}".format(os.getenv("WES_BASE"), wesId.strip())) as response:
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
            except Exception as ex:
                print("getWesRun exception for run {}: {}".format(wesId.strip(), ex))
                pass


def getWesRunIds():
    data = requests.get(os.getenv("WES_BASE")).json()
    return [run["run_id"] for run in data["runs"]]


def startWesRuns(paramsList):
    config = {
        "ALIGN_CPUS": int(os.getenv("ALIGN_CPUS", "24")),
        "MIN_PROCESS_MEM": 20,
        "SONG_URL": os.getenv("SONG_URL"),
        "SCORE_URL": os.getenv("SCORE_URL"),
        "INTERMEDIATE_SONG_URL": os.getenv("INTERMEDIATE_SONG_URL"),
        "ICGC_SCORE_URL": os.getenv("ICGC_SCORE_URL"),
        "SONG_API_TOKEN": os.getenv("SONG_API_TOKEN"),
        "ICGC_SCORE_TOKEN": os.getenv("ICGC_SCORE_API_TOKEN"),
        "WF_VERSION": os.getenv("WF_VERSION")
    }

    loop = asyncio.get_event_loop()
    coroutines = [startVariableParamsRun(params, config) for params in paramsList]
    return loop.run_until_complete(asyncio.gather(*coroutines))


async def startVariableParamsRun(params, config, semaphore=asyncio.Semaphore(1)):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            print("Starting new job for analysisId: ", params["analysisId"])

            cpus = config["ALIGN_CPUS"]
            mem = max((cpus * 3) + 2, config["MIN_PROCESS_MEM"])
            nfs = params["nfs"]
            analysisId = params["analysisId"]
            studyId = params["studyId"]
            resume = params["resume"] if "resume" in params else None

            payload = {
                "workflow_url": "icgc-argo/dna-seq-processing-wfs",
                "workflow_params": {
                    "study_id": studyId,
                    "analysis_id": analysisId,
                    "song_url": config["SONG_URL"],
                    "score_url": config["SCORE_URL"],
                    "api_token": config["SONG_API_TOKEN"],
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
                    "launch_dir": "/{}/launch".format(nfs),
                    "project_dir": "/{}/projects".format(nfs),
                    "work_dir": "/{}/work".format(nfs),
                    "revision": config["WF_VERSION"]
                }
            }

            if (resume):
                payload["workflow_engine_params"]["resume"] = resume

            if "INTERMEDIATE_SONG_URL" in config:
                payload["workflow_params"]["download"]["song_url"] = config["INTERMEDIATE_SONG_URL"]
                payload["workflow_params"]["download"]["score_url"] = config["ICGC_SCORE_URL"]
                payload["workflow_params"]["download"]["score_api_token"] = config["ICGC_SCORE_TOKEN"]

            async with session.post(os.getenv("WES_BASE"), json=payload) as response:
                data = await response.json()
                print("New run started with runId: ", data["run_id"])
                # return format for easy write into sheets as column
                return [data["run_id"]]


def processTasks(task):
    if task["state"] == "COMPLETE" and task["exit_code"] != "0":
        return {
            "process": task["process"],
            "tag": task["tag"],
            "cpus": task["cpus"],
            "memory": task["memory"],
            "duration": task["duration"],
            "realtime": task["realtime"]
        }


def extractTaskInfo(id, task):
    return [id, task["tag"], task["realtime"], task["cpus"]]


def extractRunInfo(run):
    return [extractTaskInfo(run["runId"], task) for task in run["tasks"] if task["process"] == "align"]
