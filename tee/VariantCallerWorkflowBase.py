import pandas as pd
from tee.WorkflowBase import WorkflowBase


class VariantCallerWorkflowBase(WorkflowBase):

    def __init__(self, config):
        super().__init__(config)
        self.index_cols = ["normal_aln_analysis_id", "tumour_aln_analysis_id"]

    def transformRunData(self, data):
        return {
            "normal_aln_analysis_id": data["parameters"]["normal_aln_analysis_id"],
            "tumour_aln_analysis_id": data["parameters"]["tumour_aln_analysis_id"],
            "work_dir": data["engineParameters"]["workDir"].split("/")[1],
            "run_id": data["runId"],
            "session_id": data["sessionId"],
            "state": data["state"],
            "params": data["parameters"],
            "start": self.esTimestampToLocalDate(data["startTime"]),
            "end": self.esTimestampToLocalDate(data["completeTime"]),
            "duration": float(round(int(data["duration"]) / 1000 / 60 / 60, 2)) if data["duration"] and data["duration"] != 0 else None,
        }

    def mergeRunsWithSheetData(self, runs):
        # take only the latest entry per "normal_aln_analysis_id" + "tumour_aln_analysis_id" pair (data is sorted by date at server)
        latest_runs = runs.sort_values(["start"], ascending=False).groupby(["normal_aln_analysis_id", "tumour_aln_analysis_id"]).head(1)

        # Update sheet data
        new_sheet_data = pd.merge(self.sheet_data, latest_runs[["normal_aln_analysis_id", "tumour_aln_analysis_id", "work_dir", "run_id", "session_id", "state", "start", "end", "duration"]], on=[
                                  "normal_aln_analysis_id", "tumour_aln_analysis_id"], how="left")

        new_sheet_data["work_dir"] = new_sheet_data["work_dir_y"].fillna("")
        new_sheet_data["run_id"] = new_sheet_data["run_id_y"].fillna("")
        new_sheet_data["session_id"] = new_sheet_data["session_id_y"].fillna("")
        new_sheet_data["state"] = new_sheet_data["state_y"].fillna("")
        new_sheet_data["start"] = new_sheet_data["start_y"].fillna("")
        new_sheet_data["end"] = new_sheet_data["end_y"].fillna("")
        new_sheet_data["duration"] = new_sheet_data["duration_y"].fillna("")

        return new_sheet_data.drop([
            "work_dir_x",
            "work_dir_y",
            "run_id_x",
            "run_id_y",
            "session_id_x",
            "session_id_y",
            "state_y",
            "state_x",
            "start_x",
            "start_y",
            "end_x",
            "end_y",
            "duration_x",
            "duration_y",
        ], axis=1)

    def transformEventData(self, event_data):
        # Not currently needed
        pass
