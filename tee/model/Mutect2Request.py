from tee.model.WorkflowRequestBase import WorkflowRequestBase


class Mutect2Request(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        normal_aln_analysis_id = run_config["normal_aln_analysis_id"]
        tumour_aln_analysis_id = run_config["tumour_aln_analysis_id"]
        work_dir = run_config["work_dir"]
        cpus = run_config["cpus"]
        mem = run_config["mem"]

        return {
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "study_id": study_id,
            "normal_aln_analysis_id": normal_aln_analysis_id,
            "tumour_aln_analysis_id": tumour_aln_analysis_id,
            "ref_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(work_dir),
            "mutect2_scatter_interval_files": "/{}/reference/gatk-resources/mutect2.scatter_by_chr/chr*.interval_list".format(work_dir),
            "bqsr_apply_grouping_file": "/{}/reference/gatk-resources/bqsr.sequence_grouping_with_unmapped.grch38_hla_decoy_ebv.csv".format(work_dir),
            "bqsr_recal_grouping_file": "/{}/reference/gatk-resources/bqsr.sequence_grouping.grch38_hla_decoy_ebv.csv".format(work_dir),
            "germline_resource_vcfs": ["/{}/reference/gatk-resources/af-only-gnomad.pass-only.hg38.vcf.gz".format(work_dir)],
            "contamination_variants": "/{}/reference/gatk-resources/af-only-gnomad.pass-only.biallelic.snp.hg38.vcf.gz".format(work_dir),
            "panel_of_normals": "/{}/reference/gatk-resources/1000g_pon.hg38.vcf.gz".format(work_dir),
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
            "cleanup": True
        }

    def __str__(self):
        """
        Represent and request against combo of normal_aln_analysis_id and tumour_aln_analysis_id
        """
        return "normal: {} - tumor: {}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
