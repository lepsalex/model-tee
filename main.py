from service.sheets import Sheet
from service.tee import updateSheetWithLatest, startJobsOnEmptyNFS
from time import sleep
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = "13uxLJEjv5m6Q4nNOAsgeIKs8gm2rMctBK2CDXL5c4lU"

# RANGES
RANGE = "Dev"


def model_tee():
    print("Getting sheet ...")
    # Init Spreadsheet
    sheet = Sheet(SPREADSHEET_ID)

    # Read Google Sheet into Dataframe
    sheet_data = sheet.read(RANGE)

    # Update job status
    print("Updating sheet data with latest from Cargo ...")
    sheet_data = updateSheetWithLatest(sheet_data)

    # Start jobs if possible
    print("Starting new jobs if NFS available ...")
    startJobsOnEmptyNFS(sheet_data)

    print("Sleep for 10 ...")
    for x in reversed(range(10)):
        print("."[0:1]*x, x)
        sleep(1)

    # Update again (after 10 second delay)
    sheet_data = updateSheetWithLatest(sheet_data)

    # Write sheet
    print("Writing sheet data to Google Sheets ...")
    sheet.write(RANGE, sheet_data)


while True:
    try:
        model_tee()
    except Exception as ex:
        print("Error! Restarting ... \n\n", ex)
        model_tee()

    print("Sleep for 60 ...")
    for x in reversed(range(60)):
        if x >= 10:
            if x % 10 == 0:
                print(".........", x)
        else:
            print("."[0:1]*x, x)
        sleep(1)
