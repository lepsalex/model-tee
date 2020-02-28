from service.sheets import Sheet
from service.tee import updateSheetWithLatest, startJobsOnEmptyNFS

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "13uxLJEjv5m6Q4nNOAsgeIKs8gm2rMctBK2CDXL5c4lU"

# RANGES
RANGE = "Sheet1"

# Init Spreadsheet
sheet = Sheet(SPREADSHEET_ID)

# Read Google Sheet into Dataframe
sheet_data = sheet.read(RANGE)

# Update job status
sheet_data = updateSheetWithLatest(sheet_data)
print(sheet_data)

# # Start jobs if possible
# startJobsOnEmptyNFS(sheet_data)

# Update again

# Write sheet
# sheet.write(RANGE, sheet_data)
