import pandas as pd
from tee.WorkflowBase import WorkflowBase
from tee.model.AlignRequest import AlignRequest

class AlignWorkflow(WorkflowBase):

    def __init__(self, config):
        super().__init__(config)

    def transformRunData(self, data):
        return {
            "analysis_id": data["parameters"]["analysis_id"],
            "run_id": data["runName"],
            "state": data["state"],
            "params": data["parameters"],
            "start": self.esTimestampToLocalDate(data["startTime"]),
            "end": self.esTimestampToLocalDate(data["completeTime"]),
            "duration": float(round(int(data["duration"]) / 1000 / 60 / 60, 2)) if data["duration"] and data["duration"] != 0 else None,
        }

    def mergeRunsWithSheetData(self, runs):
        # take only the latest entry per analysis_id (data is sorted by date at server)
        latest_runs = runs.sort_values(["start"], ascending=False).groupby("analysis_id").head(1)

        # Update sheet data
        new_sheet_data = pd.merge(self.sheet_data, latest_runs[["analysis_id", "run_id", "state", "start", "end", "duration"]], on="analysis_id", how="left")

        new_sheet_data["run_id"] = new_sheet_data["run_id_y"].fillna("")
        new_sheet_data["state"] = new_sheet_data["state_y"].fillna("")
        new_sheet_data["start"] = new_sheet_data["start_y"].fillna("")
        new_sheet_data["end"] = new_sheet_data["end_y"].fillna("")
        new_sheet_data["duration"] = new_sheet_data["duration_y"].fillna("")

        return new_sheet_data.drop([
            "run_id_x",
            "run_id_y",
            "state_y",
            "state_x",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "duration_x",
            "duration_y",
        ], axis=1)

    def buildRunRequests(self, run, resume=False):
        config = {
            "study_id": run["study_id"],
            "analysis_id": run["analysis_id"],
            "work_dir": run["work_dir"],
            "revision": self.wf_version,
            "cpus": int(self.cpus),
            "mem": int(self.mem)
        }

        if resume:
            config["resume"] = run["run_id"]

        return AlignRequest(self.wf_url, config)
