import os
from kafka import KafkaConsumer
from service.Kafka import Kafka
from service.CircuitBreaker import CircuitBreaker
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from tee.SangerWXSWorkflow import SangerWXSWorkflow
from dotenv import load_dotenv

# load env from file
load_dotenv()

# Build circuit breaker
circuit_breaker = CircuitBreaker(
    int(os.getenv("CB_LIMIT", 3)),
    int(os.getenv("CB_RANGE_DAYS", 2))
)

# Build workflow objects
# align_workflow = AlignWorkflow({
#     "sheet_id": os.getenv("ALIGN_SHEET_ID"),
#     "sheet_range": os.getenv("ALIGN_SHEET_RANGE"),
#     "wf_url": os.getenv("ALIGN_WF_URL"),
#     "wf_version": os.getenv("ALIGN_WF_VERSION"),
#     "max_runs": os.getenv("ALIGN_MAX_RUNS"),
#     "cpus": os.getenv("ALIGN_CPUS"),
#     "mem": os.getenv("ALIGN_MEM")
# })

sanger_wgs_workflow = SangerWGSWorkflow({
    "sheet_id": os.getenv("SANGER_WGS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WGS_WF_URL"),
    "wf_version": os.getenv("SANGER_WGS_WF_VERSION"),
    "max_runs": os.getenv("SANGER_WGS_MAX_RUNS"),
    "cpus": os.getenv("SANGER_WGS_CPUS"),
    "mem": os.getenv("SANGER_WGS_MEM")
})

# sanger_wxs_workflow = SangerWXSWorkflow({
#     "sheet_id": os.getenv("SANGER_WXS_SHEET_ID"),
#     "sheet_range": os.getenv("SANGER_WXS_SHEET_RANGE"),
#     "wf_url": os.getenv("SANGER_WXS_WF_URL"),
#     "wf_version": os.getenv("SANGER_WXS_WF_VERSION"),
#     "max_runs": os.getenv("SANGER_WXS_MAX_RUNS"),
#     "cpus": os.getenv("SANGER_WXS_CPUS"),
#     "mem": os.getenv("SANGER_WXS_MEM")
# })

def runOrUpdateFactory(wf, cb, quick=False):
    def func(quick=False):
        cb.update()

        if not cb.is_blown:
            wf.run(quick)
        else:
            print("Fuse Blown!")
            print("Error count: ".format(cb.error_count))
            wf.update()
    return func

# runOrUpdateAlign = runOrUpdateFactory(align_workflow, circuit_breaker)
runOrUpdateSangerWGX = runOrUpdateFactory(sanger_wgs_workflow, circuit_breaker)

# Message function to run on every message from Kafka on defined topic
def onMessageFunc(message):
    print("Workflow event received ... applying filter ...")

    if message.value["event"] == "completed":
        # runOrUpdateAlign(quick=False)
        runOrUpdateSangerWGX(quick=False)
    else:
        print("Event does not pass filter!")


# run on start (if we are not in circuit breaker blown state)
# runOrUpdateAlign(quick=True)
runOrUpdateSangerWGX(quick=True)

# # subscribe to workflow events and run on
# print("Waiting for workflow events ...")
# Kafka.consumeTopicWith(onMessageFunc)
