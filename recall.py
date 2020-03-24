import os
from service.sheets import Sheet
from service.tee import model_recall
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Init Spreadsheet
print("Getting sheet ...")
sheet = Sheet(SPREADSHEET_ID)

# Recall Script (to be run locally only!)
recall_list = [
    'wes-5688e42dba324c9cb3db281d7d1080cc'
]

model_recall(sheet, recall_list)
