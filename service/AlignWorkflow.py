import pandas as pd
from service.WorkflowBase import WorkflowBase
from service.request_models.AlignRequest import AlignRequest


class AlignWorkflow(WorkflowBase):

    def __init__(self, config):
        super().__init__(config)

    @classmethod
    def transformRunData(self, data):
        return {
            "analysis_id": data["request"]["workflow_params"]["analysis_id"],
            "run_id": data["run_id"],
            "state": data["state"],
            "params": data["request"]["workflow_params"],
            "start": data["run_log"]["start_time"],
            "end": data["run_log"]["end_time"],
            "duration": data["run_log"]["duration"],
            "tasks": list(filter(None, map(self.processTasks, data["task_logs"])))
        }

    def mergeRunsWithSheetData(self, runs):
        # take only the latest entry per analysis_id (data is sorted by date at server)
        latest_runs = runs.sort_values(["start"], ascending=False).groupby("analysis_id").head(1)

        # Update sheet data
        new_sheet_data = pd.merge(self.sheet_data, latest_runs[["analysis_id", "run_id", "state"]], on="analysis_id", how="left")
        new_sheet_data["run_id"] = new_sheet_data["run_id_y"].fillna(new_sheet_data["run_id_x"])
        new_sheet_data["state"] = new_sheet_data["state_y"].fillna(new_sheet_data["state_x"])

        return new_sheet_data.drop(["state_y", "state_x", "run_id_x", "run_id_y"], axis=1)

    def buildRunRequests(self, run, resume = False):
        config = {
            "study_id": run["study_id"],
            "analysis_id": run["analysis_id"],
            "work_dir": run["work_dir"],
            "max_cpus": int(self.max_cpus),
            "min_mem": 20,
        }

        if resume:
            config["resume"] = run["run_id"]

        return AlignRequest(self.wf_url, config)
