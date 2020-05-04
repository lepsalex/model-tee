import os
from tee.AlignWorkflow import AlignWorkflow
from dotenv import load_dotenv

# load env from file if present
load_dotenv(".env.prod")

# Build workflow objects
align_workflow = AlignWorkflow({
    "sheet_id": os.getenv("ALIGN_SHEET_ID"),
    "sheet_range": os.getenv("ALIGN_SHEET_RANGE"),
    "wf_url": os.getenv("ALIGN_WF_URL"),
    "wf_version": os.getenv("ALIGN_WF_VERSION"),
    "max_runs": os.getenv("ALIGN_MAX_RUNS"),
    "cpus": os.getenv("ALIGN_CPUS"),
    "mem": os.getenv("ALIGN_MEM")
})

# Recall Script (to be run locally only!)
recall_list = []

align_workflow.recall(recall_list)
