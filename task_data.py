import os
from service.sheets import Sheet
from service.tee import model_recall
from dotenv import load_dotenv
import pandas as pd
from pandas import json_normalize
from service.wes import getWesRunIds, getRunsAsDataframe

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Init Spreadsheet
print("Getting sheet ...")
sheet = Sheet(SPREADSHEET_ID)

# get all runIds
run_ids = getWesRunIds()

# get details for all runIds (we need analysisId)
latest_data = getRunsAsDataframe(run_ids)

task_data = (pd.concat({i: json_normalize(x) for i, x in latest_data.pop('tasks').items()})
         .reset_index(level=1, drop=True)
         .join(latest_data[["run_id", "analysis_id"]])
         .reset_index(drop=True))

task_data[["start", "end"]].replace('', None, inplace=True)
task_data.dropna(inplace=True)

sheet.write('Task Data!A:F', task_data[["run_id", "analysis_id", "process", "tag", "start", "end"]])
