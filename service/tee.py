import pandas as pd
from service.wes import getRunsAsDataframe

NOT_SCHEDULABLE = ['UNKNOWN', 'QUEUED', 'INITIALIZING', 'RUNNING', 'PAUSED', 'EXECUTOR_ERROR', 'SYSTEM_ERROR', 'CANCELED', 'CANCELING']

def updateSheetWithLatest(sheet_data):
    latest_data = getRunsAsDataframe(['wes-6644c438d4094bf5afb6a61f7ad36b86'])

    sheet_data = pd.merge(
        sheet_data, latest_data[['run_id', 'state']], on='run_id', how="outer")
    sheet_data['state'] = sheet_data['state_y'].fillna(sheet_data['state_x'])

    return sheet_data.drop(['state_y', 'state_x'], axis=1)

def startJobsOnEmptyNFS(sheet_data):
    # check directories that are in use
    not_schedulable_work_dirs = sheet_data.loc[sheet_data["state"].isin(NOT_SCHEDULABLE)].groupby(['work_dir'])

    # filter available directories (all dirs minus dirs in use)
    eligible_workdirs = sheet_data.loc[~sheet_data["work_dir"].isin(not_schedulable_work_dirs.groups.keys())]

    # filter out any analyses that have already been completed
    eligible_analyses = eligible_workdirs.loc[sheet_data["state"] != "COMPLETE"]

    # get one analysis per eligible work directory
    next_runs = eligible_analyses.groupby("work_dir").first()

    # build run params
    return next_runs
