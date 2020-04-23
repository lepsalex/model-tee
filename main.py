import os
from kafka import KafkaConsumer
from service.Kafka import Kafka
from tee.AlignWorkflow import AlignWorkflow
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# Build workflow objects
align_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WF_URL"),
    "wf_version": os.getenv("ALIGN_WF_VERSION"),
    "max_runs": os.getenv("ALIGN_MAX_RUNS"),
    "max_cpus": os.getenv("ALIGN_CPUS")
})

align_workflow.run(quick=True)

# sanger_workflow = AlignWorkflow({
#     "sheet_id": os.getenv("SANGER_SHEET_ID"),
#     "sheet_range": os.getenv("SANGER_SHEET_RANGE"),
#     "wf_url": os.getenv("SANGER_WF_URL"),
#     "wf_version": os.getenv("SANGER_WF_VERSION"),
#     "max_runs": os.getenv("SANGER_MAX_RUNS"),
#     "max_cpus": os.getenv("SANGER_CPUS")
# })


# # Message function to run on every message from Kafka on defined topic
# def onMessageFunc(message):
#     print("Workflow event received ... applying filter ...")

#     if message.value["event"] == "completed":
#         align_workflow.run()
#     else:
#         print("Event does not pass filter!")


# # subscribe to workflow events and run on
# print("Waiting for workflow events ...")
# Kafka.consumeTopicWith(onMessageFunc)
