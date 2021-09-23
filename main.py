import os
from multiprocessing import Process
from service.Kafka import Kafka
from service.CircuitBreaker import CircuitBreaker
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from tee.SangerWXSWorkflow import SangerWXSWorkflow
from tee.Mutect2Workflow import Mutect2Workflow
from tee.OpenAccessFiltering import OpenAccessFiltering
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
align_wgs_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_WGS_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WGS_WF_URL"),
    "wf_version": os.getenv("ALIGN_WGS_WF_VERSION"),
    "max_runs": os.getenv("ALIGN_WGS_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("ALIGN_WGS_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("ALIGN_WGS_CPUS"),
    "mem": os.getenv("ALIGN_WGS_MEM")
})

align_wxs_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_WXS_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_WXS_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WXS_WF_URL"),
    "wf_version": os.getenv("ALIGN_WXS_WF_VERSION"),
    "max_runs": os.getenv("ALIGN_WXS_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("ALIGN_WXS_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("ALIGN_WXS_CPUS"),
    "mem": os.getenv("ALIGN_WXS_MEM")
})

sanger_wgs_workflow = SangerWGSWorkflow({
    "sheet_id": os.getenv("SANGER_WGS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WGS_WF_URL"),
    "wf_version": os.getenv("SANGER_WGS_WF_VERSION"),
    "max_runs": os.getenv("SANGER_WGS_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("SANGER_WGS_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("SANGER_WGS_CPUS"),
    "pindel_cpus": os.getenv("SANGER_WGS_PINDEL_CPUS"),
    "mem": os.getenv("SANGER_WGS_MEM")
})

sanger_wxs_workflow = SangerWXSWorkflow({
    "sheet_id": os.getenv("SANGER_WXS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WXS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WXS_WF_URL"),
    "wf_version": os.getenv("SANGER_WXS_WF_VERSION"),
    "max_runs": os.getenv("SANGER_WXS_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("SANGER_WXS_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("SANGER_WXS_CPUS"),
    "mem": os.getenv("SANGER_WXS_MEM")
})

mutect2_workflow = Mutect2Workflow({
    "sheet_id": os.getenv("MUTECT2_SHEET_ID"),
    "sheet_range": os.getenv("MUTECT2_SHEET_RANGE"),
    "wf_url": os.getenv("MUTECT2_WF_URL"),
    "wf_version": os.getenv("MUTECT2_WF_VERSION"),
    "max_runs": os.getenv("MUTECT2_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("MUTECT2_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("MUTECT2_CPUS"),
    "mem": os.getenv("MUTECT2_MEM"),
    "bqsr": False
})

open_filter_workflow = OpenAccessFiltering({
    "sheet_id": os.getenv("OPEN_FILTER_SHEET_ID"),
    "sheet_range": os.getenv("OPEN_FILTER_SHEET_RANGE"),
    "wf_url": os.getenv("OPEN_FILTER_WF_URL"),
    "wf_version": os.getenv("OPEN_FILTER_WF_VERSION"),
    "max_runs": os.getenv("OPEN_FILTER_MAX_RUNS"),
    "max_runs_per_dir": os.getenv("OPEN_FILTER_MAX_RUNS_PER_DIR"),
    "cpus": os.getenv("OPEN_FILTER_CPUS"),
    "mem": os.getenv("OPEN_FILTER_MEM")
})

runOrUpdateAlignWGS = Utils.methodOrUpdateFactory(align_wgs_workflow, "run", circuit_breaker)
runOrUpdateAlignWXS = Utils.methodOrUpdateFactory(align_wxs_workflow, "run", circuit_breaker)
runOrUpdateSangerWGS = Utils.methodOrUpdateFactory(sanger_wgs_workflow, "run", circuit_breaker)
runOrUpdateSangerWXS = Utils.methodOrUpdateFactory(sanger_wxs_workflow, "run", circuit_breaker)
runOrUpdateMutect2 = Utils.methodOrUpdateFactory(mutect2_workflow, "run", circuit_breaker)
runOrUpdateOpenFilter = Utils.methodOrUpdateFactory(open_filter_workflow, "run", circuit_breaker)

# Global count disabled (for now)
# getMergeRunCounts = Utils.mergeRunCountsFuncGen(align_wgs_workflow, align_wxs_workflow, sanger_wgs_workflow,
#                                                 sanger_wxs_workflow, mutect2_workflow, open_filter_workflow)

getMergeWorkDirsInUse = Utils.mergeWorkDirsInUseFuncGen(align_wgs_workflow, align_wxs_workflow,
                                                        sanger_wgs_workflow, sanger_wxs_workflow, mutect2_workflow, open_filter_workflow)


def onWorkflowMessageFunc(message):
    print("Workflow event received ... applying filter ...")

    if message.value["event"] == "completed":
        print("Workflow event valid, starting configured processes ...")
        runOrUpdateAlignWGS(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(align_wgs_workflow))

        runOrUpdateAlignWXS(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(align_wxs_workflow))

        runOrUpdateSangerWGS(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(sanger_wgs_workflow))

        runOrUpdateSangerWXS(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(sanger_wxs_workflow))

        runOrUpdateMutect2(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(mutect2_workflow))

        runOrUpdateOpenFilter(quick=False, global_work_dirs_in_use=getMergeWorkDirsInUse(open_filter_workflow))
    else:
        print("Workflow event does not pass filter!")


# Processes
workflowConsumer = Process(target=Kafka.consumeTopicWith, args=(os.getenv("KAFKA_TOPIC", "workflow"), onWorkflowMessageFunc))

# Main
if __name__ == '__main__':
    # run on start (if we are not in circuit breaker blown state)
    runOrUpdateAlignWGS(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(align_wgs_workflow))

    runOrUpdateAlignWXS(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(align_wxs_workflow))

    runOrUpdateSangerWGS(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(sanger_wgs_workflow))

    runOrUpdateSangerWXS(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(sanger_wxs_workflow))

    runOrUpdateMutect2(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(mutect2_workflow))

    runOrUpdateOpenFilter(quick=True, global_work_dirs_in_use=getMergeWorkDirsInUse(open_filter_workflow))

    # subscribe to workflow events and run
    print("Waiting for workflow events ...")
    workflowConsumer.start()
