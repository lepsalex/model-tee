import os
from kafka import KafkaConsumer
from service.sheets import Sheet
from service.tee import modelTee, printStartScreen
from service.kafka import consumeTopicWith
from time import sleep
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# The ID and range of the spreadsheet.
SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Init Spreadsheet
print("Getting sheet ...")
sheet = Sheet(SPREADSHEET_ID)

# run once on startup
modelTee(sheet)

# Message function to run on every message from Kafka on defined topic
def onMessageFunc(message):
    print("Workflow event received ... applying filter ...")

    if message.value["event"] == "completed":
        print("\nModel T roll out!")
        printStartScreen()
        modelTee(sheet)
    else:
        print("Event does not pass filter!")


# subscribe to workflow events and run on
print("Waiting for workflow events ...")
consumeTopicWith(onMessageFunc)
