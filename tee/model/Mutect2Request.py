from tee.model.WorkflowRequestBase import WorkflowRequestBase


class Mutect2Request(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        analysis_id = run_config["analysis_id"]
        work_dir = run_config["work_dir"]
        cpus = run_config["cpus"]
        mem = run_config["mem"]

        return {
            "study_id": study_id,
            "analysis_id": analysis_id,
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "ref_genome_fa": "/{}/reference/MUTECT2-PLACEHOLDER/MUTECT2-PLACEHOLDER.fa".format(work_dir),
            "cpus": cpus,
            "mem": mem,
            "cleanup": True
        }

    def __str__(self):
        """
        Represent and request against analysis_id
        """
        return self.wp_config["analysis_id"]
