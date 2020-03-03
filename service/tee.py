import pandas as pd
from service.wes import getWesRunIds, getRunsAsDataframe

NOT_SCHEDULABLE = ['UNKNOWN', 'QUEUED', 'INITIALIZING', 'RUNNING', 'PAUSED', 'EXECUTOR_ERROR', 'SYSTEM_ERROR', 'CANCELED', 'CANCELING']

def updateSheetWithLatest(sheet_data):
    # get all runIds
    run_ids = getWesRunIds()

    # get details for all runIds (we need analysisId)
    latest_data = getRunsAsDataframe(run_ids)

    # take only the latest entry per analysis_id (data is sorted by date at server)
    latest_data = latest_data.sort_values(['start']).groupby("analysis_id").head(1)

    # Update sheet
    sheet_data = pd.merge(
        sheet_data, latest_data[['analysis_id', 'run_id', 'state']], on='analysis_id', how="left")
    sheet_data['run_id'] = sheet_data['run_id_y'].fillna(sheet_data['run_id_x'])
    sheet_data['state'] = sheet_data['state_y'].fillna(sheet_data['state_x'])

    return sheet_data.drop(['state_y', 'state_x', 'run_id_x', 'run_id_y'], axis=1)

def startJobsOnEmptyNFS(sheet_data):
    # check directories that are in use
    not_schedulable_work_dirs = sheet_data.loc[sheet_data["state"].isin(NOT_SCHEDULABLE)].groupby(['work_dir'])

    # filter available directories (all dirs minus dirs in use)
    eligible_workdirs = sheet_data.loc[~sheet_data["work_dir"].isin(not_schedulable_work_dirs.groups.keys())]

    # filter out any analyses that have already been completed
    eligible_analyses = eligible_workdirs.loc[sheet_data["state"] != "COMPLETE"]

    # get one analysis per eligible work directory
    next_runs = eligible_analyses.groupby("work_dir").first()

    # TODO: build run params
    print(next_runs)
