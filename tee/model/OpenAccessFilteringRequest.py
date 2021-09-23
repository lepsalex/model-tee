from tee.model.WorkflowRequestBase import WorkflowRequestBase


class OpenAccessFilteringRequest(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        analysis_id = run_config["analysis_id"]
        cpus = run_config["cpus"]
        mem = run_config["mem"]

        params = {
            "study_id": study_id,
            "analysis_id": analysis_id,
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "regions_file": "/<SCHEDULED_DIR>/reference/open-access-filter/open_access.gencode_v38.20210915.bed.gz",
            "cpus": cpus,
            "mem": mem,
            "cleanup": True,
            "max_retries": 3,
            "first_retry_wait_time": 5
        }

        return params

    def __str__(self):
        """
        Represent and request against analysis_id
        """
        return self.wp_config["analysis_id"]
