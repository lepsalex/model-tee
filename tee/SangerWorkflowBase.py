import pandas as pd
from tee.WorkflowBase import WorkflowBase
from tee.model.AlignRequest import AlignRequest


class SangerWorkflowBase(WorkflowBase):

    def __init__(self, config):
        super().__init__(config)

    def transformRunData(self, data):
        return {
            "normal_aln_analysis_id": data["request"]["workflow_params"]["normal_aln_analysis_id"],
            "tumour_aln_analysis_id": data["request"]["workflow_params"]["tumour_aln_analysis_id"],
            "run_id": data["run_id"],
            "state": data["state"],
            "params": data["request"]["workflow_params"],
            "start": data["run_log"]["start_time"],
            "end": data["run_log"]["end_time"],
            "duration": round(data["run_log"]["duration"] / 1000 / 60 / 60, 2) if data["run_log"]["duration"] and data["run_log"]["duration"] != 0 else None,
            "tasks": list(filter(None, map(self.processTasks, data["task_logs"])))
        }

    def mergeRunsWithSheetData(self, runs):
        # take only the latest entry per "normal_aln_analysis_id" + "tumour_aln_analysis_id" pair (data is sorted by date at server)
        latest_runs = runs.sort_values(["start"], ascending=False).groupby(["normal_aln_analysis_id", "tumour_aln_analysis_id"]).head(1)

        # Update sheet data
        new_sheet_data = pd.merge(self.sheet_data, latest_runs[["normal_aln_analysis_id", "tumour_aln_analysis_id", "run_id", "state", "start", "end", "duration"]], on=[
                                  "normal_aln_analysis_id", "tumour_aln_analysis_id"], how="left")

        new_sheet_data["run_id"] = new_sheet_data["run_id_y"].fillna(new_sheet_data["run_id_x"]).fillna("")
        new_sheet_data["state"] = new_sheet_data["state_y"].fillna(new_sheet_data["state_x"]).fillna("")
        new_sheet_data["start"] = new_sheet_data["start_y"].fillna(new_sheet_data["start_x"]).fillna("")
        new_sheet_data["end"] = new_sheet_data["end_y"].fillna(new_sheet_data["end_x"]).fillna("")
        new_sheet_data["duration"] = new_sheet_data["duration_y"].fillna(new_sheet_data["duration_x"]).fillna("")

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
