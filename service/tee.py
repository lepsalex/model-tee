import os
import pandas as pd
from time import sleep
from service.wes import getWesRunIds, getRunsAsDataframe, startWesRuns

NOT_SCHEDULABLE = ["QUEUED", "INITIALIZING", "RUNNING", "CANCELING"]
ALREADY_RAN = ["COMPLETE", "SYSTEM_ERROR", "EXECUTOR_ERROR", "UNKNOWN"]


def modelTee(sheet):
    RANGE = os.getenv("ALIGN_SHEET_RANGE")
    ALIGN_MAX_RUNS = os.getenv("ALIGN_MAX_RUNS")

    # Read Google Sheet into Dataframe
    sheet_data = sheet.read(RANGE)

    # Update job status
    print("Updating sheet data with latest from Cargo ...")
    sheet_data = updateSheetWithLatest(sheet_data)

    # Compute job availability (ALIGN_MAX_RUNS - Current Running Jobs)
    current_run_count = sheet_data.groupby("state")["state"].count().get("RUNNING", 0)
    run_availability = int(ALIGN_MAX_RUNS) - int(current_run_count)

    # Start new jobs if there is room
    if (run_availability > 0):
        # Start jobs if possible
        print("Starting new jobs if NFS available ...")
        startJobsOnEmptyNFS(sheet_data, run_availability)

        # Update again (after 20 second delay)
        printSleepForN(20)
        sheet_data = updateSheetWithLatest(sheet_data)
    else:
        print("WES currently at max run capacity ({})".format(ALIGN_MAX_RUNS))

    # Write sheet
    print("Writing sheet data to Google Sheets ...")
    sheet.write(RANGE, sheet_data)


def updateSheetWithLatest(sheet_data):
    # get all runIds
    run_ids = getWesRunIds()

    # if there is no data, just return the sheet data
    if len(run_ids) == 0:
        print("Warning: No runs!")
        return sheet_data

    # get details for all runIds (we need analysisId)
    latest_data = getRunsAsDataframe(run_ids)

    # if we don't have any non-buggy runs exit
    if latest_data.size == 0:
        print("Warning: Non-buggy runs == 0")
        return sheet_data

    # take only the latest entry per analysis_id (data is sorted by date at server)
    latest_data = latest_data.sort_values(["start"], ascending=False).groupby("analysis_id").head(1)

    # Update sheet
    sheet_data = pd.merge(
        sheet_data, latest_data[["analysis_id", "run_id", "state"]], on="analysis_id", how="left")
    sheet_data["run_id"] = sheet_data["run_id_y"].fillna(
        sheet_data["run_id_x"])
    sheet_data["state"] = sheet_data["state_y"].fillna(sheet_data["state_x"])

    return sheet_data.drop(["state_y", "state_x", "run_id_x", "run_id_y"], axis=1)


def startJobsOnEmptyNFS(sheet_data, run_availability):
    # get a list of optional dir we want to not schedule jobs on
    TAINTED_DIR_LIST = os.getenv("TAINTED_DIR_LIST", "").split(",")

    # check directories that are in use
    not_schedulable_work_dirs = sheet_data.loc[sheet_data["state"].isin(NOT_SCHEDULABLE)].groupby(["work_dir"])

    # filter available directories (set of all dirs minus dirs in use + tainted dirs from env)
    unavailable_dir = {y for x in [not_schedulable_work_dirs.groups.keys(), TAINTED_DIR_LIST] for y in x if y}
    eligible_workdirs = sheet_data.loc[~sheet_data["work_dir"].isin(unavailable_dir)]

    # filter out any analyses that have already been completed
    eligible_analyses = eligible_workdirs.loc[~sheet_data["state"].isin(ALREADY_RAN)]

    # NOTE: ".sample(...)" is used below in order pull a random work_dir from the list otherwise
    # we would always be scheduling primarily on NFS-1/NFS-2 until all those runs were complete and then on
    # NFS-3/NFS-4, not that it would necessarily be a problem but would like to see more normal distribution

    # get one analysis per eligible work directory
    # (limit to lesser of: total amount of runs possible vs. run_availability)
    next_runs = eligible_analyses.groupby("work_dir").first().reset_index()
    next_runs = next_runs.sample(min(run_availability, next_runs.shape[0]))

    # build run params
    params = [computeParams(next_run) for next_run in next_runs.values.tolist()]

    # start runs
    newRuns = startWesRuns(params)


def computeParams(next_run):
    return {
        "nfs": next_run[0],
        "studyId": next_run[7],
        "analysisId": next_run[8]
    }


def computeRerunParams(next_run):
    return {
        "studyId": next_run[6],
        "analysisId": next_run[7],
        "nfs": next_run[8],
        "resume": next_run[9]
    }


def printSleepForN(n=10):
    print("Sleep for {} ...".format(n))
    for x in reversed(range(n)):
        print("."[0:1]*min(x, 9), x)
        sleep(1)


def printStartScreen():
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


def modelRecall(sheet, runIds):
    RANGE = os.getenv("ALIGN_SHEET_RANGE")

    # Read Google Sheet into Dataframe
    sheet_data = sheet.read(RANGE)

    retry_runs = sheet_data.loc[sheet_data["run_id"].isin(runIds)]

    # build run params as
    params = [computeRerunParams(retry_run) for retry_run in retry_runs.values.tolist()]

    # start re-runs
    newRuns = startWesRuns(params)

    # Update again (after 20 second delay)
    printSleepForN(20)
    sheet_data = updateSheetWithLatest(sheet_data)

    # Write sheet
    print("Writing sheet data to Google Sheets ...")
    sheet.write(RANGE, sheet_data)
