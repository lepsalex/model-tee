import pandas as pd
from tee.SangerWorkflowBase import SangerWorkflowBase
from tee.model.SangerWGSRequest import SangerWGSRequest


class SangerWGSWorkflow(SangerWorkflowBase):

    def __init__(self, config):
        super().__init__(config)

    def buildRunRequests(self, run, resume=False):
        config = {
            "study_id": run["study_id"],
            "normal_aln_analysis_id": run["normal_aln_analysis_id"],
            "tumour_aln_analysis_id": run["tumour_aln_analysis_id"],
            "work_dir": run["work_dir"],
            "revision": self.wf_version,
            "max_cpus": int(self.max_cpus),
            "min_mem": 20,
        }

        if resume:
            config["resume"] = run["run_id"]

        return SangerWGSRequest(self.wf_url, config)
