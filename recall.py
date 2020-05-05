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
recall_list = [
    "74e49f21-677e-4882-93bb-7c3534744cd4",
    "62649b5e-81cd-4e50-9817-346f53f0bc30",
    "215a4109-ccb3-40a9-94f2-3f18b4cc2b7e"   
]

align_workflow.recall(recall_list)
