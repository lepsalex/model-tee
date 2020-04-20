import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from time import sleep
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
    def mergeRunsWithSheetData(self, runs):
        """
        Defines how run data is merged with the sheet for
        this workflow, can (and does) vary with workflow params
        """
        pass

    @abstractmethod
    def formatRunData(self, data):
        """
        Defines how to parse wes response data for this workflow.
        Must be implemented, called from fetchWesRun()
        """
        pass

    def run(self):
        # get latest run info for sheet data
        self.sheet_data = self.__updateSheetWithWesData()

        # Compute job availability
        run_availability = self.__computeRunAvailability()

        # Start new jobs if there is room
        if (run_availability > 0):
            # Start jobs if possible
            print("Starting new jobs if NFS available ...")

            # Update again (after 20 second delay)
            self.printSleepForN(20)
            self.sheet_data = self.__updateSheetWithWesData()
        else:
            print("WES currently at max run capacity ({})".format(self.max_runs))

        print(self.sheet_data.head())
        print(run_availability)

    def __updateSheetWithWesData(self):
        runs = self.__fetchWesRunsAsDataframe()

        # if we don't have any runs exit
        if runs.size == 0:
            print("Warning: no runs returned, defaulting to existing sheet data!")
            return self.sheet_data

        return self.mergeRunsWithSheetData(runs)

    def __fetchWesRunsAsDataframe(self):
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

    def __computeRunAvailability(self):
        """
        Compute job availability (ALIGN_MAX_RUNS - Current Running Jobs)
        """
        current_run_count = self.sheet_data.groupby("state")["state"].count().get("RUNNING", 0)
        return int(self.max_runs) - int(current_run_count)

    def __printSleepForN(self, n=10):
        print("Sleep for {} ...".format(n))
        for x in reversed(range(n)):
            print("."[0:1]*min(x, 9), x)
            sleep(1)

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


class AlignWorkflow(Workflow):

    def __init__(self, config):
        super().__init__(config)

    def mergeRunsWithSheetData(self, runs):
        # take only the latest entry per analysis_id (data is sorted by date at server)
        latest_runs = runs.sort_values(["start"], ascending=False).groupby("analysis_id").head(1)

        # Update sheet data
        new_sheet_data = pd.merge(self.sheet_data, latest_runs[["analysis_id", "run_id", "state"]], on="analysis_id", how="left")
        new_sheet_data["run_id"] = new_sheet_data["run_id_y"].fillna(new_sheet_data["run_id_x"])
        new_sheet_data["state"] = new_sheet_data["state_y"].fillna(new_sheet_data["state_x"])

        return new_sheet_data.drop(["state_y", "state_x", "run_id_x", "run_id_y"], axis=1)

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
