import os
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

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

# Recall Script (to be run locally only!)
recall_list = []

align_wgs_workflow.update()
# align_wgs_workflow.recall(recall_list)

# align_wxs_workflow.update()
# align_wxs_workflow.recall(recall_list)

# sanger_wgs_workflow.update()
# sanger_wgs_workflow.recall(recall_list)
