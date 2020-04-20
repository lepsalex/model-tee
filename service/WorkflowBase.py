import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from time import sleep
from abc import ABC, abstractmethod
from service.sheets import Sheet


class WorkflowBase(ABC):

    NOT_SCHEDULABLE = ["QUEUED", "INITIALIZING", "RUNNING", "CANCELING"]
    ALREADY_RAN = ["COMPLETE", "SYSTEM_ERROR", "EXECUTOR_ERROR", "UNKNOWN"]

    def __init__(self, config):
        # config
        self.sheet_id = config["sheet_id"]
        self.sheet_range = config["sheet_range"]
        self.wf_url = config["wf_url"]
        self.wf_version = config["wf_version"]
        self.max_runs = config["max_runs"]
        self.max_cpus = config["max_cpus"]

        # general env config (not specific to any workflow)
        self.tainted_dir_list = os.getenv("TAINTED_DIR_LIST", "").split(",")

        # initial state
        self.sheet = Sheet(self.sheet_id)
        self.sheet_data = self.sheet.read(self.sheet_range)

    @abstractmethod
    def formatRunData(self, data):
        """
        Defines how to parse wes response data for this workflow.
        Must be implemented, called from fetchWesRun()
        """
        pass

    @abstractmethod
    def mergeRunsWithSheetData(self, runs):
        """
        Defines how run data is merged with the sheet for
        this workflow, can (and does) vary with workflow params
        """
        pass

    @abstractmethod
    def computeNewRunParams(self, data):
        """
        Method that creates a list of parameter dictionaries,
        each entry representing the params for a new run
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
            self.__startJobsOnEmptyNFS(run_availability)

            # Update again (after 20 second delay)
            self.__printSleepForN(20)
            self.sheet_data = self.__updateSheetWithWesData()
        else:
            print("WES currently at max run capacity ({})".format(self.max_runs))

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
        current_run_count = self.sheet_data.groupby("state")["state"].count().get("RUNNING", 0) # too magic
        return int(self.max_runs) - int(current_run_count)

    def __startJobsOnEmptyNFS(self, run_availability):
        # check directories that are in use
        not_schedulable_work_dirs = self.sheet_data.loc[self.sheet_data["state"].isin(self.NOT_SCHEDULABLE)].groupby(["work_dir"])

        # filter available directories (set of all dirs minus dirs in use + tainted dirs from env)
        unavailable_dir = {y for x in [not_schedulable_work_dirs.groups.keys(), self.tainted_dir_list] for y in x if y}
        eligible_workdirs = self.sheet_data.loc[~self.sheet_data["work_dir"].isin(unavailable_dir)]

        # filter out any analyses that have already been completed
        eligible_analyses = eligible_workdirs.loc[~self.sheet_data["state"].isin(self.ALREADY_RAN)]

        # NOTE: ".sample(...)" is used below in order pull a random work_dir from the list otherwise
        # we would always be scheduling primarily on NFS-1/NFS-2 until all those runs were complete and then on
        # NFS-3/NFS-4, not that it would necessarily be a problem but would like to see more normal distribution

        # get one analysis per eligible work directory
        # (limit to lesser of: total amount of runs possible vs. run_availability)
        next_runs = eligible_analyses.groupby("work_dir").first().reset_index()
        next_runs = next_runs.sample(min(run_availability, next_runs.shape[0]))

        # build run params
        params = [self.computeNewRunParams(next_run) for next_run in next_runs.iterrows()]

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
