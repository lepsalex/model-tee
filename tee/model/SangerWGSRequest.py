from tee.model.WorkflowRequestBase import WorkflowRequestBase


class SangerWGSRequest(WorkflowRequestBase):
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
                "score_cpus": 8,
                "score_mem": 18
            },
            "sangerWgsVariantCaller": {
                "cpus": cpus,
                "mem": mem,
                "pindelcpu": 3,
                "ref_genome_tar": "/{}/reference/sanger-variant-calling/core_ref_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir),
                "vagrent_annot": "/{}/reference/sanger-variant-calling/VAGrENT_ref_GRCh38_hla_decoy_ebv_ensembl_91.tar.gz".format(work_dir),
                "ref_snv_indel_tar": "/{}/reference/sanger-variant-calling/SNV_INDEL_ref_GRCh38_hla_decoy_ebv-fragment.tar.gz".format(work_dir),
                "ref_cnv_sv_tar": "/{}/reference/sanger-variant-calling/CNV_SV_ref_GRCh38_hla_decoy_ebv_brass6+.tar.gz".format(work_dir),
                "qcset_tar": "/{}/reference/sanger-variant-calling/qcGenotype_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir)
            },
            "generateBas": {
                "cpus": 6,
                "mem": 32,
                "ref_genome_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(work_dir)
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
                "score_cpus": 8,
                "score_mem": 18
            },
            "cleanup": True
        }

    def __str__(self):
        """
        Represent and request against combo of normal_aln_analysis_id and tumour_aln_analysis_id
        """
        return "normal: {} - tumor: {}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
