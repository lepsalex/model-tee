import os
from multiprocessing import Process
from kafka import KafkaConsumer
from service.Kafka import Kafka
from service.CircuitBreaker import CircuitBreaker
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from tee.SangerWXSWorkflow import SangerWXSWorkflow
from tee.CovidWorkflow import CovidWorkflow
from tee.Utils import Utils
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
#     "max_runs_per_dir": os.getenv("ALIGN_MAX_RUNS_PER_DIR"),
#     "cpus": os.getenv("ALIGN_CPUS"),
#     "mem": os.getenv("ALIGN_MEM")
# })

# sanger_wgs_workflow = SangerWGSWorkflow({
#     "sheet_id": os.getenv("SANGER_WGS_SHEET_ID"),
#     "sheet_range": os.getenv("SANGER_WGS_SHEET_RANGE"),
#     "wf_url": os.getenv("SANGER_WGS_WF_URL"),
#     "wf_version": os.getenv("SANGER_WGS_WF_VERSION"),
#     "max_runs": os.getenv("SANGER_WGS_MAX_RUNS"),
#     "max_runs_per_dir": os.getenv("SANGER_WGS_MAX_RUNS_PER_DIR"),
#     "cpus": os.getenv("SANGER_WGS_CPUS"),
#     "mem": os.getenv("SANGER_WGS_MEM")
# })

# sanger_wxs_workflow = SangerWXSWorkflow({
#     "sheet_id": os.getenv("SANGER_WXS_SHEET_ID"),
#     "sheet_range": os.getenv("SANGER_WXS_SHEET_RANGE"),
#     "wf_url": os.getenv("SANGER_WXS_WF_URL"),
#     "wf_version": os.getenv("SANGER_WXS_WF_VERSION"),
#     "max_runs": os.getenv("SANGER_WXS_MAX_RUNS"),
#     "max_runs_per_dir": os.getenv("SANGER_WxS_MAX_RUNS_PER_DIR"),
#     "cpus": os.getenv("SANGER_WXS_CPUS"),
#     "mem": os.getenv("SANGER_WXS_MEM")
# })

covid_workflow = CovidWorkflow({
    "sheet_id": os.getenv("COVID_SHEET_ID"),
    "sheet_range": os.getenv("COVID_SHEET_RANGE"),
    "wf_url": os.getenv("COVID_WF_URL"),
    "wf_version": os.getenv("COVID_WF_VERSION"),
    "max_runs": os.getenv("COVID_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("COVID_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("COVID_CPUS"),
    "mem": os.getenv("COVID_MEM")
})

# runOrUpdateAlign = Utils.methodOrUpdateFactory(align_workflow, "run", circuit_breaker)
# runOrUpdateSangerWGX = Utils.methodOrUpdateFactory(sanger_wgs_workflow, "run", circuit_breaker)
appendAndRunCovid = Utils.methodOrUpdateFactory(covid_workflow, "appendAndRun", circuit_breaker)

# def onWorkflowMessageFunc(message):
#     print("Workflow event received ... applying filter ...")

#     if message.value["event"] == "completed":
#         # runOrUpdateAlign(quick=False)
#         runOrUpdateSangerWGX(quick=False)
#     else:
#         print("Workflow event does not pass filter!")

def onSongMessageFunc(message):
    print("SONG event received ... applying filter ...")

    if message.value["state"] == "PUBLISHED":
        # runOrUpdateAlign(quick=False)
        appendAndRunCovid(date=message.value, quick=False)
    else:
        print("SONG event does not pass filter!")

if __name__ == '__main__':
    # run on start (if we are not in circuit breaker blown state)
    # runOrUpdateAlign(quick=True)
    # runOrUpdateSangerWGX(quick=True)

    appendAndRunCovid(data=Utils.generateTestSongPubEvent(), quick=True)

    # # subscribe to workflow events and run
    # print("Waiting for workflow events ...")
    # workflowConsumer = Process(target=Kafka.consumeTopicWith, args=(os.getenv("KAFKA_TOPIC", "workflow"), onWorkflowMessageFunc))
    # workflowConsumer.start()
