import pandas as pd
from tee.VariantCallerWorkflowBase import VariantCallerWorkflowBase
from tee.model.Mutect2Request import Mutect2Request


class Mutect2Workflow(VariantCallerWorkflowBase):

    def __init__(self, config):
        super().__init__(config)
        self.bqsr = config["bqsr"]

    def buildRunRequests(self, run, resume=False):
        config = {
            "study_id": run["study_id"],
            "normal_aln_analysis_id": run["normal_aln_analysis_id"],
            "tumour_aln_analysis_id": run["tumour_aln_analysis_id"],
            "work_dir": run["work_dir"],
            "revision": self.wf_version,
            "cpus": int(self.cpus),
            "mem": int(self.mem),
            "bqsr": self.bqsr
        }

        if resume:
            config["resume"] = run["session_id"]

        return Mutect2Workflow(self.wf_url, config)
