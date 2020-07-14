# Model-T(ee)

Very basic workflow orchestration/automation using the Google Sheets API and the OICR RDPC Workflow Execution System (WES). The project is intended to be a stop-gap allowing us to run genomic workflows while we build out the grander automation solution. Basic flow is as follows:

1. User prepared input into Google Sheet
2. Model-tee initiates a workflow given some params (from .env)
```
    align_workflow = AlignWorkflow({
        "sheet_id": os.getenv("ALIGN_WGS_SHEET_ID"),
        "sheet_range": os.getenv("ALIGN_WGS_SHEET_RANGE"),
        "wf_url": os.getenv("ALIGN_WGS_WF_URL"),
        "wf_version": os.getenv("ALIGN_WGS_WF_VERSION"),
        "max_runs": os.getenv("ALIGN_WGS_MAX_RUNS"),
        "cpus": os.getenv("ALIGN_WGS_CPUS"),
        "mem": os.getenv("ALIGN_WGS_MEM")
    })
```
3. Model-tee reads sheet (Google API) into a dataframe (Pandas)
4. Model-tee executes a graphQL (WES API) query and merges latest run data with sheets data
5. Logic to determine run availability and build run queue
6. Run parameters built (unique per workflow) and runs are initiated
7. Updated sheets data is written back into the Google sheet

Is this perfect, no there are any number of potential pitfalls and problems with using a Google sheets as essentially on of your databases and UI's. Does it work? Absolutely and has already successfully completed hundreds if not thousands of runs.

#### Could this project be easily adapted to someone elses needs?
If you are running OICR RDPC software this would be extremely easy to adapt for your specific workflows. If you have some other open-source WES or even a proprietary system it may require additional modification but really the core concept is that we tie workflow runs (WES) with some metadata system (SONG) on some key (usually analysisId).

#### Is this production ready?
This by it's nature is a pre-production solution so the straight answer is no. That said it has been running production workflows, in a production cluster, for the better part of 3-months with little to no issues.

### What is the (ee) in T(ee)?
Enterprise Edition ... just kidding, I wanted a "package name" that was shorter than Model-T and without a hyphen ... well T is to short so the phonetic spelling (in my head at least) came out to `tee`. Mystery solved.
