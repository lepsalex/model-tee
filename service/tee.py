import os
import pandas as pd
from time import sleep
from service.wes import getWesRunIds, getRunsAsDataframe, startWesRuns

RANGE = os.getenv("GOOGLE_SHEET_RANGE", "Dev")

NOT_SCHEDULABLE = ["QUEUED", "INITIALIZING", "RUNNING", "CANCELING"]
ALREADY_RAN = ["COMPLETE", "SYSTEM_ERROR", "EXECUTOR_ERROR"]

CPUS = 24


def model_tee(sheet):
    # Read Google Sheet into Dataframe
    sheet_data = sheet.read(RANGE)

    # Update job status
    print("Updating sheet data with latest from Cargo ...")
    sheet_data = updateSheetWithLatest(sheet_data)

    # Start jobs if possible
    print("Starting new jobs if NFS available ...")
    startJobsOnEmptyNFS(sheet_data)

    print("Sleep for 10 ...")
    for x in reversed(range(10)):
        print("."[0:1]*x, x)
        sleep(1)

    # Update again (after 10 second delay)
    sheet_data = updateSheetWithLatest(sheet_data)

    # Write sheet
    print("Writing sheet data to Google Sheets ...")
    sheet.write(RANGE, sheet_data)


def updateSheetWithLatest(sheet_data):
    # get all runIds
    run_ids = getWesRunIds()

    # get details for all runIds (we need analysisId)
    latest_data = getRunsAsDataframe(run_ids)

    # take only the latest entry per analysis_id (data is sorted by date at server)
    latest_data = latest_data.sort_values(
        ["start"]).groupby("analysis_id").head(1)

    # Update sheet
    sheet_data = pd.merge(
        sheet_data, latest_data[["analysis_id", "run_id", "state"]], on="analysis_id", how="left")
    sheet_data["run_id"] = sheet_data["run_id_y"].fillna(
        sheet_data["run_id_x"])
    sheet_data["state"] = sheet_data["state_y"].fillna(sheet_data["state_x"])

    return sheet_data.drop(["state_y", "state_x", "run_id_x", "run_id_y"], axis=1)


def startJobsOnEmptyNFS(sheet_data):
    # check directories that are in use
    not_schedulable_work_dirs = sheet_data.loc[sheet_data["state"].isin(
        NOT_SCHEDULABLE)].groupby(["work_dir"])

    # filter available directories (all dirs minus dirs in use)
    eligible_workdirs = sheet_data.loc[~sheet_data["work_dir"].isin(
        not_schedulable_work_dirs.groups.keys())]

    # filter out any analyses that have already been completed
    eligible_analyses = eligible_workdirs.loc[~sheet_data["state"].isin(ALREADY_RAN)]

    # get one analysis per eligible work directory
    next_runs = eligible_analyses.groupby("work_dir").first().reset_index()

    # build run params
    params = [computeParams(next_run) for next_run in next_runs.values.tolist()]

    newRuns = startWesRuns(params)

    print("New runs started: ", newRuns[0])


def computeParams(next_run):
    return {
        "cpus": CPUS,
        "nfs": next_run[0],
        "studyId": next_run[7],
        "analysisId": next_run[8]
    }
