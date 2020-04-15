import os
from service.sheets import Sheet
from service.tee import modelRecall
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Init Spreadsheet
print("Getting sheet ...")
sheet = Sheet(SPREADSHEET_ID)

# Recall Script (to be run locally only!)
recall_list = []

modelRecall(sheet, recall_list)
