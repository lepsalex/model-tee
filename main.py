import pandas as pd
from service.sheets import Sheet
from service.wes import getRunsAsDataframe

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "13uxLJEjv5m6Q4nNOAsgeIKs8gm2rMctBK2CDXL5c4lU"

# RANGES
RANGE = "Sheet1"

# Init Spreadsheet
sheet = Sheet(SPREADSHEET_ID)

# Read Google Sheet into Dataframe
sheet_data = sheet.read(RANGE)

# Update job status
latest_data = getRunsAsDataframe(['wes-6644c438d4094bf5afb6a61f7ad36b86'])

sheet_data = pd.merge(
    sheet_data, latest_data[['run_id', 'state']], on='run_id', how="outer")
sheet_data['state'] = sheet_data['state_y'].fillna(sheet_data['state_x'])
sheet_data = sheet_data.drop(['state_y', 'state_x'], axis=1)

# Start jobs if possible

# Update again

# Write sheet
sheet.write(RANGE, sheet_data)
