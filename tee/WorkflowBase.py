import os
import pytz
import pandas as pd
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

    def __init__(self, config):
        print("Workflow init for wf_url: ", config["wf_url"])

        # config
        self.sheet_id = config["sheet_id"]
        self.sheet_range = config["sheet_range"]
        self.wf_url = config["wf_url"]
        self.wf_version = config["wf_version"]
        self.max_runs = config["max_runs"]
        self.max_runs_per_dir = config["max_runs_per_dir"]
        self.cpus = config["cpus"]
        self.mem = config["mem"]

        # general env config (not specific to any workflow)
        self.tainted_dir_list = os.getenv("TAINTED_DIR_LIST", "").split(",")

        # initial state
        self.sheet = Sheet(self.sheet_id)
        self.sheet_data = self.sheet.read(self.sheet_range)
        self.gql_query = self.__gqlQueryBuilder()
        self.run_count = self.__getCurrentRunCount()
        self.work_dirs_in_use = self.__getWorkdirsInUse()
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

    def run(self, quick=False, global_run_count=0, global_work_dirs_in_use=[]):

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

    def recall(self, run_ids):
        # get latest run info for sheet data
        sheet_data = self.__updateSheetWithWesData()

        retry_runs = sheet_data.loc[sheet_data["run_id"].isin(run_ids)]

        requests = [self.buildRunRequests(retry_run[1], True) for retry_run in retry_runs.iterrows()]

        Wes.startWesRuns(requests)

        # Update again (after 30 second delay)
        self.__printSleepForN(30)
        sheet_data = self.__updateSheetWithWesData()

        # Write sheet
        print("Writing sheet data to Google Sheets ...")
        self.sheet.write(self.sheet_range, sheet_data)

    def appendAndRun(self, data, quick=False, global_run_count=0, global_work_dirs_in_use=[]):
        # Print logogram if not in quick mode
        if not quick:
            self.__printStartScreen()

        # get latest run info for sheet data
        self.sheet_data = self.__updateSheetWithWesData()

        # transform and append event-data as row


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
                runName
                state
                parameters
                startTime
                completeTime
                duration
            }
        }
        ''' % self.wf_url)

    def __updateSheetWithWesData(self):
        runs = Wes.fetchWesRunsAsDataframeForWorkflow(self.gql_query, self.transformRunData, self.index_cols)

        # if we don't have any runs exit
        if runs.size == 0:
            print("Warning: no runs returned, defaulting to existing sheet data!")
            return self.sheet_data

        return self.mergeRunsWithSheetData(runs)

    def __getCurrentRunCount(self):
        """
        Get count of currently running jobs for THIS workflow
        """
        return self.sheet_data.groupby("state")["state"].count().get("RUNNING", 0)

    def __computeRunAvailability(self, global_run_count):
        """
        Compute job availability (ALIGN_MAX_RUNS - Current Running Jobs (__getCurrentRunCount))
        """
        return int(self.max_runs) - (int(self.__getCurrentRunCount()) + global_run_count)

    def __getWorkdirsInUse(self):
        return self.sheet_data[self.sheet_data["state"] == "RUNNING"]["work_dir"].values

    def __computeUnavailableWorkDirs(self, acc, curr):
        """
        Computes list of unavailable work_dir(s) as a list of dirs
        have >= X jobs running where X is the max_runs_per_dir param
        """
        if (curr[1] >= int(self.max_runs_per_dir)):
            acc.append(curr[0])

        return acc

    def __startJobsOnAvailableNFS(self, run_availability, global_work_dirs_in_use=[]):
        # get directories that are in use for this workflow
        not_schedulable_work_dirs = self.sheet_data.loc[self.sheet_data["state"].isin(self.NOT_SCHEDULABLE)].groupby(["work_dir"])

        # get group size and pass to reduce to get unavailable directories (# jobs in dir >= max_runs_per_dir)
        not_schedulable_work_dirs = reduce(self.__computeUnavailableWorkDirs, not_schedulable_work_dirs.size().iteritems(), [])

        # combine with globally used work_dirs (if any)
        not_schedulable_work_dirs = set(not_schedulable_work_dirs + global_work_dirs_in_use)

        # filter available directories (set of all dirs minus dirs in use + tainted dirs from env)
        unavailable_dir = {y for x in [not_schedulable_work_dirs, self.tainted_dir_list] for y in x if y}
        eligible_workdirs = self.sheet_data.loc[~self.sheet_data["work_dir"].isin(unavailable_dir)]

        # filter out any analyses that have already been completed or are currently running
        eligible_analyses = eligible_workdirs.loc[~self.sheet_data["state"].isin(self.ALREADY_RAN + self.NOT_SCHEDULABLE)]

        # NOTE: ".sample(...)" is used below in order pull a random work_dir from the list otherwise
        # we would always be scheduling primarily on NFS-1/NFS-2 until all those runs were complete and then on
        # NFS-3/NFS-4, not that it would necessarily be a problem but would like to see more normal distribution

        # get one analysis per eligible work directory
        # (limit to lesser of: total amount of runs possible vs. run_availability)
        next_runs = eligible_analyses.groupby("work_dir").first().reset_index()
        next_runs = next_runs.sample(min(run_availability, next_runs.shape[0]))

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
