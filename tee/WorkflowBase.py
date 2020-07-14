import os
import pytz
import uuid
import random
import operator
import pandas as pd
from collections import Counter
from random import randint
from functools import reduce
from datetime import datetime
from time import sleep
from abc import ABC, abstractmethod
from gql import gql
from repository.Sheet import Sheet
from service.Wes import Wes


class WorkflowBase(ABC):

    NOT_SCHEDULABLE = ["QUEUED", "INITIALIZING", "RUNNING", "CANCELING"]
    ALREADY_RAN = ["COMPLETE", "SYSTEM_ERROR", "EXECUTOR_ERROR", "UNKNOWN"]

    # 16 work dirs in total following this format
    WORK_DIRS = {"nfs-{}-c{}".format(x, y) for x in range(1, 5) for y in range(1, 5)}

    def __init__(self, config):
        print("Workflow init for wf_url: ", config["wf_url"])

        # config
        self.sheet_id = config["sheet_id"]
        self.sheet_range = config["sheet_range"]
        self.wf_url = config["wf_url"]
        self.wf_version = config["wf_version"]
        self.max_runs = int(config["max_runs"])
        self.max_runs_per_dir = int(config["max_runs_per_dir"])
        self.cpus = int(config["cpus"])
        self.mem = config["mem"]

        # general env config (not specific to any workflow)
        self.tainted_dir_list = os.getenv("TAINTED_DIR_LIST", "").split(",")

        # initial state
        self.sheet = Sheet(self.sheet_id)
        self.sheet_data = self.sheet.read(self.sheet_range)
        self.gql_query = self.__gqlQueryBuilder()
        self.work_dirs_in_use = self.__getWorkdirsInUse()
        self.run_count = self.__getCurrentRunCount()
        self.work_dir_inventory = self.__buildWorkdirInventory()
        self.index_cols = None

    @abstractmethod
    def transformRunData(self, gql_run):
        """
        Converts raw GQL response into format we
        can merge with what is in the Sheet
        """
        pass

    @abstractmethod
    def transformEventData(self, event_data):
        """
        Converts raw event data (usually from SONG)
        so it can merge (append) to what is in the Sheet
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
    def buildRunRequests(self, run, resume):
        """
        Must return instance of WorkflowRequest derived class
        """
        pass

    def run(self, quick=False, global_run_count=0, global_work_dirs_in_use=Counter()):

        # Print logogram if not in quick mode
        if not quick:
            self.__printStartScreen()

        # get latest run info for sheet data
        self.sheet_data = self.__updateSheetWithWesData()

        # Compute job availability
        run_availability = self.__computeRunAvailability(global_run_count)

        # Start new jobs if there is room
        if (run_availability > 0):
            # Start jobs if possible
            print("Starting new jobs if NFS available ...")
            self.__startJobsOnAvailableNFS(run_availability, global_work_dirs_in_use)

            # Update again (after 30 second delay)
            self.__printSleepForN(30)
            self.sheet_data = self.__updateSheetWithWesData()
        else:
            print("WES currently at max run capacity ({})".format(self.max_runs))

        # Update state
        self.run_count = self.__getCurrentRunCount()
        self.work_dirs_in_use = self.__getWorkdirsInUse()

        # Write sheet
        print("Writing sheet data to Google Sheets ...")
        self.sheet.write(self.sheet_range, self.sheet_data)

    def update(self):
        """
        Does a quick spreadsheet update with the latest from WES
        """
        # get latest run info for sheet data
        self.sheet_data = self.__updateSheetWithWesData()

        # Write sheet
        print("Writing sheet data to Google Sheets ...")
        self.sheet.write(self.sheet_range, self.sheet_data)

    def recall(self, session_ids):
        # get latest run info for sheet data
        sheet_data = self.__updateSheetWithWesData()

        retry_runs = sheet_data.loc[sheet_data["session_id"].isin(session_ids)]

        requests = [self.buildRunRequests(retry_run[1], True) for retry_run in retry_runs.iterrows()]

        Wes.startWesRuns(requests)

        # Update again (after 30 second delay)
        self.__printSleepForN(30)
        sheet_data = self.__updateSheetWithWesData()

        # Write sheet
        print("Writing sheet data to Google Sheets ...")
        self.sheet.write(self.sheet_range, sheet_data)

    def appendAndRun(self, data, quick=False, global_run_count=0, global_work_dirs_in_use=Counter()):
        # Print logogram if not in quick mode
        if not quick:
            self.__printStartScreen()

        try:
            # transform and append event-data as row
            event_data = self.transformEventData(data)
            row = pd.Series(data=event_data, name=str(uuid.uuid4()))
            self.sheet_data = self.sheet_data.append(row, verify_integrity=True)
        except ValueError as err:
            print("Cannot append row to sheet, duplicate key detected.\n\Data: {}\n\nError: {}".format(row, err))

        # get latest run info for sheet data
        self.sheet_data = self.__updateSheetWithWesData()

        # TODO: Finish up testing and make a real feature
        # print(self.sheet_data)

        # # Compute job availability
        # run_availability = self.__computeRunAvailability(global_run_count)

        # # Start new jobs if there is room
        # if (run_availability > 0):
        #     # Start jobs if possible
        #     print("Starting new jobs if NFS available ...")
        #     self.__startJobsOnAvailableNFS(run_availability, global_work_dirs_in_use)

        #     # Update again (after 30 second delay)
        #     self.__printSleepForN(30)
        #     self.sheet_data = self.__updateSheetWithWesData()
        # else:
        #     print("WES currently at max run capacity ({})".format(self.max_runs))

        # # Update state
        # self.run_count = self.__getCurrentRunCount()
        # self.work_dirs_in_use = self.__getWorkdirsInUse()

        # # Write sheet
        # print("Writing sheet data to Google Sheets ...")
        # self.sheet.write(self.sheet_range, self.sheet_data)

    @property
    def run_count(self):
        return self.__run_count

    @property
    def work_dirs_in_use(self):
        return self.__work_dirs_in_use

    @run_count.setter
    def run_count(self, run_count):
        self.__run_count = run_count

    @work_dirs_in_use.setter
    def work_dirs_in_use(self, work_dirs_in_use):
        self.__work_dirs_in_use = work_dirs_in_use

    def __gqlQueryBuilder(self):
        return gql('''
        {
            runs(page: {from: 0, size: 1000}, filter: {repository:\"%s\"} ) {
                runId
                sessionId
                state
                parameters
                engineParameters {
                    workDir
                }
                startTime
                completeTime
                duration
            }
        }
        ''' % self.wf_url)

    def __updateSheetWithWesData(self):
        df = self.sheet_data
        runs = Wes.fetchWesRunsAsDataframeForWorkflow(self.gql_query, self.transformRunData, self.index_cols)

        # Merge with WES data if it exists
        if runs.size == 0:
            print("Warning: no runs returned, defaulting to existing sheet data!")
            df.fillna(value="", inplace=True)
        else:
            df = self.mergeRunsWithSheetData(runs)

        return df

    def __getCurrentRunCount(self):
        """
        Get count of currently running or scheduled jobs for THIS workflow
        """
        return sum(self.work_dirs_in_use.values())

    def __computeRunAvailability(self, global_run_count):
        """
        Compute job availability (ALIGN_WGS_MAX_RUNS - Current Running Jobs (__getCurrentRunCount))
        """
        return self.max_runs - (self.__getCurrentRunCount() + global_run_count)

    def __buildWorkdirInventory(self):
        return Counter({work_dir: self.max_runs_per_dir for work_dir in self.WORK_DIRS if work_dir not in self.tainted_dir_list})

    def __getWorkdirsInUse(self):
        if self.sheet_data.size > 0:
            work_dirs_in_use = self.sheet_data.loc[self.sheet_data["state"].isin(self.NOT_SCHEDULABLE)].groupby(["work_dir"])
            return Counter(dict((x, y) for x, y in work_dirs_in_use.size().iteritems()))
        else:
            return {}

    def __assignWorkDir(self, row, available_work_dirs):
        idx = int(row.name)
        if (idx < len(available_work_dirs)):
            row["work_dir"] = available_work_dirs[idx]
            return row

    def __startJobsOnAvailableNFS(self, run_availability, global_work_dirs_in_use):
        # get directories that are in use for this workflow combined with global work_dir usage
        work_dirs_in_use = self.__getWorkdirsInUse() + global_work_dirs_in_use

        # subtract work_dirs_in_use Counter from self.work_dir_inventory to get available work_dirs with counts
        available_work_dirs = self.work_dir_inventory - work_dirs_in_use

        # turn available_work_dirs into randomly ordered list of work_dirs
        # with duplicates representing count of availability
        available_work_dirs = reduce(operator.add, [[work_dir for count in range(count)] for work_dir, count in available_work_dirs.items()])
        random.shuffle(available_work_dirs)

        # get next analyses by filtering out any analyses that have already been completed or are
        # currently running and taking the next n rows from that group, where n == run_availability
        eligible_analyses = self.sheet_data.loc[~self.sheet_data["state"].isin(self.ALREADY_RAN + self.NOT_SCHEDULABLE)].head(run_availability).reset_index()

        # assign work_dirs and drop any analyses that don't get a work_dir (once all are assigned from available_work_dirs)
        next_runs = eligible_analyses.apply(self.__assignWorkDir, axis=1, available_work_dirs=available_work_dirs).dropna(subset=["work_dir"])

        # build run requests (iterrows returns tuple, [1] is where the data is)
        requests = [self.buildRunRequests(next_run[1], resume=False) for next_run in next_runs.iterrows()]

        Wes.startWesRuns(requests)

    def __printSleepForN(self, n=10):
        print("Sleep for {} ...".format(n))
        for x in reversed(range(n)):
            print("."[0:1]*min(x, 9), x)
            sleep(1)

    @classmethod
    def esTimestampToLocalDate(cls, timestamp):
        eastern = pytz.timezone('US/Eastern')
        ts = int(timestamp) / 1000 if timestamp else None
        return format(eastern.localize(datetime.fromtimestamp(ts))) if ts else None

    @classmethod
    def __printStartScreen(cls):
        print("\nModel T roll out!")

        logo_gram = [
            "\n",
            "██──▀██▀▀▀██▀──██",
            "█▀█▄──▀█▄█▀──▄█▀█",
            "██▄▀█▄──▀──▄█▀▄██",
            "█▄▀█▄█─█▄█─█▄█▀▄█",
            "─▀█▄██─███─██▄█▀─",
            "█──────▐█▌──────█",
            "██▌─▄█─███─█▄─▐██",
            "██▌▐██─▀▀▀─██▌▐██",
            "██▌▐█████████▌▐█▀",
            "▀█▌▐██─▄▄▄─██▌▐█─",
            "─▀──█▌▐███▌▐█──▀─",
            "──────█████──────",
            "\n"
        ]

        for line in logo_gram:
            print(line)
            sleep(0.7)
