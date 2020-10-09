import os
from tee.AlignWorkflow import AlignWorkflow
from tee.SangerWGSWorkflow import SangerWGSWorkflow
from tee.SangerWXSWorkflow import SangerWXSWorkflow
from tee.Mutect2Workflow import Mutect2Workflow
from dotenv import load_dotenv

# load env from file if present
load_dotenv()

# Build workflow objects
align_wgs_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_WGS_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WGS_WF_URL"),
    "wf_version": os.getenv("ALIGN_WGS_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("ALIGN_WGS_CPUS"),
    "mem": os.getenv("ALIGN_WGS_MEM")
})

align_wxs_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_WXS_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_WXS_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WXS_WF_URL"),
    "wf_version": os.getenv("ALIGN_WXS_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("ALIGN_WXS_CPUS"),
    "mem": os.getenv("ALIGN_WXS_MEM")
})


sanger_wgs_workflow = SangerWGSWorkflow({
    "sheet_id": os.getenv("SANGER_WGS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WGS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WGS_WF_URL"),
    "wf_version": os.getenv("SANGER_WGS_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("SANGER_WGS_CPUS"),
    "mem": os.getenv("SANGER_WGS_MEM")
})

sanger_wxs_workflow = SangerWXSWorkflow({
    "sheet_id": os.getenv("SANGER_WXS_SHEET_ID"),
    "sheet_range": os.getenv("SANGER_WXS_SHEET_RANGE"),
    "wf_url": os.getenv("SANGER_WXS_WF_URL"),
    "wf_version": os.getenv("SANGER_WXS_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("SANGER_WXS_CPUS"),
    "mem": os.getenv("SANGER_WXS_MEM")
})

mutect2_workflow = Mutect2Workflow({
    "sheet_id": os.getenv("MUTECT2_SHEET_ID"),
    "sheet_range": os.getenv("MUTECT2_SHEET_RANGE"),
    "wf_url": os.getenv("MUTECT2_WF_URL"),
    "wf_version": os.getenv("MUTECT2_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("MUTECT2_CPUS"),
    "mem": os.getenv("MUTECT2_MEM"),
    "bqsr": False
})

mutect2_bqsr_workflow = Mutect2Workflow({
    "sheet_id": os.getenv("MUTECT2_BQSR_SHEET_ID"),
    "sheet_range": os.getenv("MUTECT2_BQSR_SHEET_RANGE"),
    "wf_url": os.getenv("MUTECT2_BQSR_WF_URL"),
    "wf_version": os.getenv("MUTECT2_BQSR_WF_VERSION"),
    "max_runs": -1,
    "max_runs_per_dir": -1,
    "cpus": os.getenv("MUTECT2_BQSR_CPUS"),
    "mem": os.getenv("MUTECT2_BQSR_MEM"),
    "bqsr": True
})

# Recall Script (to be run locally only!)
recall_list = []

# align_wgs_workflow.update()
# align_wgs_workflow.recall(recall_list)

# align_wxs_workflow.update()
# align_wxs_workflow.recall(recall_list)

# sanger_wgs_workflow.update()
# sanger_wgs_workflow.recall(recall_list)

# sanger_wxs_workflow.update()
# sanger_wxs_workflow.recall(recall_list)

# mutect2_workflow.update()
# mutect2_workflow.recall(recall_list)

# mutect2_bqsr_workflow.update()
# mutect2_bqsr_workflow.recall(recall_list)
