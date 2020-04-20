import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from service.sheets import Sheet


class Workflow(ABC):

    def __init__(self, config):
        # config
        self.sheet_id = config["sheet_id"]
        self.sheet_range = config["sheet_range"]
        self.wf_url = config["wf_url"]
        self.wf_version = config["wf_version"]
        self.max_runs = config["max_runs"]
        self.max_cpus = config["max_cpus"]

        # initial state
        self.sheet = Sheet(self.sheet_id)
        self.sheet_data = self.sheet.read(self.sheet_range)

    @abstractmethod
    def updateSheetWithWesData(self):
        pass

    # must be implemented, called from fetchWesRun()
    @abstractmethod
    def formatRunData(self, data):
        pass

    def fetchWesRunsAsDataframe(self):
        try:
            # get runIds from WES (can throw ValueError on no runs)
            runIds = self.__fetchWesRunIds()

            # get data for runs belonging to this workflow
            loop = asyncio.get_event_loop()
            coroutines = [self.__fetchWesRun(runId) for runId in runIds]
            runs = [run for run in loop.run_until_complete(asyncio.gather(*coroutines)) if run]

            return pd.DataFrame(runs)
        except ValueError as err:
            # log error and return empty dataframe
            print(err)
            return pd.DataFrame()

    def processTasks(self, task):
        """
        Tasks are universal between workflows for our purposed,
        this utility method can be called by any implementing
        class if needed
        """
        if task["state"] == "COMPLETE" and task["exit_code"] != "0":
            return {
                "process": task["process"],
                "tag": task["tag"],
                "cpus": task["cpus"],
                "memory": task["memory"],
                "duration": task["duration"],
                "realtime": task["realtime"],
                "start": task["start_time"],
                "end": task["end_time"]
            }

    def __fetchWesRunIds(self):
        data = requests.get(os.getenv("WES_BASE")).json()
        run_ids = [run["run_id"] for run in data["runs"]]

        if len(run_ids) == 0:
            raise ValueError("No runs exist in WES!")

        return run_ids

    async def __fetchWesRun(self, wesId):
        """
        Returns a run only if it is for this workflow,
        otherwise returns False
        """
        async with aiohttp.ClientSession() as session:
            async with session.get("{}/{}".format(os.getenv("WES_BASE"), wesId.strip())) as response:
                data = await response.json()

                if data["request"]["workflow_url"] == self.wf_url:
                    return self.formatRunData(data)
                else:
                    return False


class AlignWorkflow(Workflow):

    def __init__(self, config):
        super().__init__(config)

    def updateSheetWithWesData(self):
        runs = self.fetchWesRunsAsDataframe()
        print(runs.head())

    def formatRunData(self, data):
        return {
            "analysis_id": data["request"]["workflow_params"]["analysis_id"],
            "run_id": data["run_id"],
            "state": data["state"],
            "params": data["request"]["workflow_params"],
            "start": data["run_log"]["start_time"],
            "end": data["run_log"]["end_time"],
            "duration": data["run_log"]["duration"],
            "tasks": list(filter(None, map(self.processTasks, data["task_logs"])))
        }
