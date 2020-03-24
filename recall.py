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
    'wes-9ab89f2a1e824065b4cd0c56701c225b',
    'wes-61627ab0fc4d42d1aaa3e94b168e9639'
]

model_recall(sheet, recall_list)
