import os
from service.sheets import Sheet
from service.tee import model_tee
from time import sleep
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID", "13uxLJEjv5m6Q4nNOAsgeIKs8gm2rMctBK2CDXL5c4lU")

# Settings
INTERVAL = int(os.getenv("RUN_INTERVAL_SECONDS", 60))

print("Getting sheet ...")
# Init Spreadsheet
sheet = Sheet(SPREADSHEET_ID)

while True:
    try:
        model_tee(sheet)
    except Exception as ex:
        print("Error! Restarting ... \n\n", ex)
        model_tee(sheet)

    print("Sleep for {} ...".format(INTERVAL))
    for x in reversed(range(INTERVAL)):
        if x >= 10:
            if x % 10 == 0:
                print(".........", x)
        else:
            print("."[0:1]*x, x)
        sleep(1)
