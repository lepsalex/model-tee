from tee.model.WorkflowRequestBase import WorkflowRequestBase


class AlignRequest(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        analysis_id = run_config["analysis_id"]
        work_dir = run_config["work_dir"]
        cpus = run_config["cpus"]
        mem = max((cpus * 3) + 2, run_config["mem"])

        params = {
            "study_id": study_id,
            "analysis_id": analysis_id,
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "api_token": song_score_config["SONG_API_TOKEN"],
            "ref_genome_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(work_dir),
            "cpus": 2,
            "mem": 4,
            "download": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 8,
                "score_mem": 18
            },
            "seqDataToLaneBam": {
                "cpus": 4,
                "mem": 12
            },
            "bwaMemAligner": {
                "cpus": cpus,
                "mem": mem
            },
            "bamMergeSortMarkdup": {
                "cpus": 4,
                "mem": 18
            },
            "payloadGenDnaAlignment": {
                "cpus": 2,
                "mem": 4
            },
            "readGroupUBamQC": {
                "cpus": 3,
                "mem": 6
            },
            "alignedSeqQC": {
                "cpus": 4,
                "mem": 10
            },
            "gatkCollectOxogMetrics": {
                "cpus": 3,
                "mem": 6
            },
            "payloadGenDnaSeqQc": {
                "cpus": 2,
                "mem": 2
            },
            "uploadAlignment": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 8,
                "score_mem": 18
            },
            "uploadQc": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 2,
                "score_mem": 4
            },
            "cleanup": True
        }

        if song_score_config.get("INTERMEDIATE_SONG_URL"):
            params["download"]["song_url"] = song_score_config["INTERMEDIATE_SONG_URL"]
            params["download"]["score_url"] = song_score_config["ICGC_SCORE_URL"]
            params["download"]["score_api_token"] = song_score_config["ICGC_SCORE_TOKEN"]

        return params

    def __str__(self):
        """
        Represent and request against analysis_id
        """
        return self.wp_config["analysis_id"]
