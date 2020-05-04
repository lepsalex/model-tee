from tee.model.WorkflowRequestBase import WorkflowRequestBase


class SangerWXSRequest(WorkflowRequestBase):
    def __init__(self, workflow_name, config=None):
        super().__init__(workflow_name, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        normal_aln_analysis_id = run_config["normal_aln_analysis_id"]
        tumour_aln_analysis_id = run_config["tumour_aln_analysis_id"]
        work_dir = run_config["work_dir"]
        cpus = run_config["cpus"]
        mem = run_config["mem"]

        params = {
            "study_id": study_id,
            "normal_aln_analysis_id": normal_aln_analysis_id,
            "tumour_aln_analysis_id": tumour_aln_analysis_id,
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "api_token": song_score_config["SONG_API_TOKEN"],
            "cpus": 1,
            "mem": 1,
            "download": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 8,
                "score_mem": 18
            },
            "sangerWxsVariantCaller": {
                "cpus": cpus,
                "mem": mem,
                "exclude": "chr1,chr2,chr3,chr4,chr5,chr6,chr7,chr8,chr9,chr10,chr11,chr12,chr13,chr14,chr15,chr16,chr17,chr18,chr19,chr20,chr22,chrX,chrY,chrUn%,HLA%,%_alt,%_random,chrM,chrEBV",
                "vagrent_annot": "/{}/reference/sanger-variant-calling/VAGrENT_ref_GRCh38_hla_decoy_ebv_ensembl_91.tar.gz".format(work_dir),
                "ref_genome_tar": "/{}/reference/sanger-variant-calling/core_ref_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir),
                "ref_snv_indel_tar": "/{}/reference/sanger-variant-calling/SNV_INDEL_ref_GRCh38_hla_decoy_ebv-fragment.tar.gz".format(work_dir),
            },
            "generateBas": {
                "ref_genome_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(work_dir)
            },
            "repackSangerResults": {
                "cpus": 1,
                "mem": 1
            },
            "repack-sanger-results": {
                "cpus": 1,
                "mem": 1
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
        Represent and request against combo of normal_aln_analysis_id and tumour_aln_analysis_id
        """
        return "normal: {} - tumor: {}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
