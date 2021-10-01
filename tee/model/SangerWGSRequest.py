from tee.model.WorkflowRequestBase import WorkflowRequestBase


class SangerWGSRequest(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        normal_aln_analysis_id = run_config["normal_aln_analysis_id"]
        tumour_aln_analysis_id = run_config["tumour_aln_analysis_id"]
        cpus = run_config["cpus"]
        pindel_cpus = run_config["pindel_cpus"]
        mem = run_config["mem"]

        return {
            "study_id": study_id,
            "normal_aln_analysis_id": normal_aln_analysis_id,
            "tumour_aln_analysis_id": tumour_aln_analysis_id,
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "cpus": 2,
            "mem": 6,
            "download": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 4,
                "score_mem": 10
            },
            "sangerWgsVariantCaller": {
                "cpus": cpus,
                "mem": mem,
                "pindelcpu": pindel_cpus,
                "ref_genome_tar": "<SCHEDULED_DIR>/reference/sanger-variant-calling/core_ref_GRCh38_hla_decoy_ebv.tar.gz",
                "vagrent_annot": "<SCHEDULED_DIR>/reference/sanger-variant-calling/VAGrENT_ref_GRCh38_hla_decoy_ebv_ensembl_91.tar.gz",
                "ref_snv_indel_tar": "<SCHEDULED_DIR>/reference/sanger-variant-calling/SNV_INDEL_ref_GRCh38_hla_decoy_ebv-fragment.tar.gz",
                "ref_cnv_sv_tar": "<SCHEDULED_DIR>/reference/sanger-variant-calling/CNV_SV_ref_GRCh38_hla_decoy_ebv_brass6+.tar.gz",
                "qcset_tar": "<SCHEDULED_DIR>/reference/sanger-variant-calling/qcGenotype_GRCh38_hla_decoy_ebv.tar.gz"
            },
            "generateBas": {
                "cpus": 6,
                "mem": 32,
                "ref_genome_fa": "<SCHEDULED_DIR>/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa"
            },
            "repackSangerResults": {
                "cpus": 2,
                "mem": 4
            },
            "prepSangerSupplement": {
                "cpus": 2,
                "mem": 8
            },
            "cavemanVcfFix": {
                "cpus": 2,
                "mem": 16
            },
            "extractSangerCall": {
                "cpus": 2,
                "mem": 4
            },
            "payloadGenVariantCall": {
                "cpus": 2,
                "mem": 8
            },
            "prepSangerQc": {
                "cpus": 2,
                "mem": 8
            },
            "upload": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 4,
                "score_mem": 10
            },
            "cleanup": True,
            "max_retries": 5,
            "first_retry_wait_time": 60
        }

    def __str__(self):
        """
        Represent and request against combo of normal_aln_analysis_id and tumour_aln_analysis_id
        """
        return "normal: {} - tumor: {}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
