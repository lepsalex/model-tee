from tee.model.WorkflowRequestBase import WorkflowRequestBase


class Mutect2Request(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        normal_aln_analysis_id = run_config["normal_aln_analysis_id"]
        tumour_aln_analysis_id = run_config["tumour_aln_analysis_id"]
        cpus = run_config["cpus"]
        mem = run_config["mem"]
        bqsr = run_config["bqsr"]

        return {
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "study_id": study_id,
            "normal_aln_analysis_id": normal_aln_analysis_id,
            "tumour_aln_analysis_id": tumour_aln_analysis_id,
            "ref_fa": "<SCHEDULED_DIR>/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa",
            "mutect2_scatter_interval_files": "<SCHEDULED_DIR>/reference/gatk-resources/mutect2.scatter_by_chr/chr*.interval_list",
            "bqsr_apply_grouping_file": "<SCHEDULED_DIR>/reference/gatk-resources/bqsr.sequence_grouping_with_unmapped.grch38_hla_decoy_ebv.csv",
            "bqsr_recal_grouping_file": "<SCHEDULED_DIR>/reference/gatk-resources/bqsr.sequence_grouping.grch38_hla_decoy_ebv.csv",
            "germline_resource_vcfs": "<SCHEDULED_DIR>/reference/gatk-resources/af-only-gnomad.pass-only.hg38.vcf.gz",
            "contamination_variants": "<SCHEDULED_DIR>/reference/gatk-resources/af-only-gnomad.pass-only.biallelic.snp.hg38.vcf.gz",
            "panel_of_normals": "<SCHEDULED_DIR>/reference/gatk-resources/1000g_pon.hg38.vcf.gz",
            "cpus": cpus,
            "mem": mem,
            "download": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 4,
                "score_mem": 10
            },
            "calculateContamination": {
                "mem": 10
            },
            "perform_bqsr": bqsr,
            "cleanup": True,
            "max_retries": 5,
            "first_retry_wait_time": 60
        }

    def __str__(self):
        """
        Represent and request against combo of normal_aln_analysis_id and tumour_aln_analysis_id
        """
        return "normal: {} - tumor: {}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
