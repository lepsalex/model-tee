import os
import numpy as np
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

# Read Google Sheet into Dataframe
sheet_data = sheet.read("Report!A:F")

# get all runIds that have status COMPLETE
run_ids = sheet_data.loc[sheet_data["state"] == "COMPLETE"]["run_id"].values

# get details for runIds
run_data = getRunsAsDataframe(run_ids)

# convert time to hours
run_data["duration"] = run_data["duration"].map(lambda m: round(m / 1000 / 60 / 60, 2))

# join sheet data
write_data = pd.merge(sheet_data, run_data[["run_id", "start", "end", "duration"]], on="run_id", how="left", sort=False)
write_data = write_data.replace(np.nan, '', regex=True)

# print to report sheet
sheet.write('Report!H:J', write_data[["start", "end", "duration"]])
