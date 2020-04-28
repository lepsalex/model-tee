import os
from kafka import KafkaConsumer
from service.Kafka import Kafka
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from tee.SangerWXSWorkflow import SangerWXSWorkflow
from dotenv import load_dotenv

# load env from file if present
load_dotenv(".env")

# Build workflow objects
align_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WF_URL"),
    "wf_version": os.getenv("ALIGN_WF_VERSION"),
    "max_runs": os.getenv("ALIGN_MAX_RUNS"),
    "max_cpus": os.getenv("ALIGN_CPUS")
})

sanger_wgs_workflow = SangerWGSWorkflow({
    "sheet_id": os.getenv("SANGER_WGS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WGS_WF_URL"),
    "wf_version": os.getenv("SANGER_WGS_WF_VERSION"),
    "max_runs": os.getenv("SANGER_WGS_MAX_RUNS"),
    "max_cpus": os.getenv("SANGER_WGS_CPUS")
})

sanger_wxs_workflow = SangerWXSWorkflow({
    "sheet_id": os.getenv("SANGER_WXS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WXS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WXS_WF_URL"),
    "wf_version": os.getenv("SANGER_WXS_WF_VERSION"),
    "max_runs": os.getenv("SANGER_WXS_MAX_RUNS"),
    "max_cpus": os.getenv("SANGER_WXS_CPUS")
})

# run on start
align_workflow.run(True, global_work_dirs_in_use=['nfs-2-c2', 'nfs-3-c3'])
print(align_workflow.run_count)
print(align_workflow.work_dirs_in_use)

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
