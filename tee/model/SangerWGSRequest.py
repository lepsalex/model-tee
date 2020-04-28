from tee.model.WorkflowRequestBase import WorkflowRequestBase


class SangerWGSRequest(WorkflowRequestBase):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildWorkflowParams(self, run_config, song_score_config):
        study_id = run_config["study_id"]
        normal_aln_analysis_id = run_config["normal_aln_analysis_id"],
        tumour_aln_analysis_id = run_config["tumour_aln_analysis_id"],
        work_dir = run_config["work_dir"]
        cpus = run_config["max_cpus"]
        mem = max((cpus * 3) + 2, run_config["min_mem"])

        params = {
            "study_id": study_id,
            "normal_aln_analysis_id": normal_aln_analysis_id,
            "tumour_aln_analysis_id": tumour_aln_analysis_id,
            "ref_genome_fa": "/{}/reference/GRCh38_hla_decoy_ebv/GRCh38_hla_decoy_ebv.fa".format(work_dir),
            "song_url": song_score_config["SONG_URL"],
            "score_url": song_score_config["SCORE_URL"],
            "api_token": song_score_config["SONG_API_TOKEN"],
            "cpus": 2,
            "mem": 6,
            "download": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 8,
                "score_mem": 18
            },
            "generateBas": {
                "cpus": 6,
                "mem": 8
            },
            "generate-bas": {
                "cpus": 6,
                "mem": 8
            },
            "sangerWgsVariantCaller": {
                "cpus": 12,
                "mem": 108,
                "ref_genome_tar": "/{}/reference/sanger-variant-calling/core_ref_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir),
                "vagrent_annot": "/{}/reference/sanger-variant-calling/VAGrENT_ref_GRCh38_hla_decoy_ebv_ensembl_91.tar.gz".format(work_dir),
                "ref_snv_indel_tar": "/{}/reference/sanger-variant-calling/SNV_INDEL_ref_GRCh38_hla_decoy_ebv-fragment.tar.gz".format(work_dir),
                "ref_cnv_sv_tar": "/{}/reference/sanger-variant-calling/CNV_SV_ref_GRCh38_hla_decoy_ebv_brass6+.tar.gz".format(work_dir),
                "qcset_tar": "/{}/reference/sanger-variant-calling/qcGenotype_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir)
            },
            "sanger-wgs-variant-caller": {
                "cpus": 12,
                "mem": 108,
                "ref_genome_tar": "/{}/reference/sanger-variant-calling/core_ref_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir),
                "vagrent_annot": "/{}/reference/sanger-variant-calling/VAGrENT_ref_GRCh38_hla_decoy_ebv_ensembl_91.tar.gz".format(work_dir),
                "ref_snv_indel_tar": "/{}/reference/sanger-variant-calling/SNV_INDEL_ref_GRCh38_hla_decoy_ebv-fragment.tar.gz".format(work_dir),
                "ref_cnv_sv_tar": "/{}/reference/sanger-variant-calling/CNV_SV_ref_GRCh38_hla_decoy_ebv_brass6+.tar.gz".format(work_dir),
                "qcset_tar": "/{}/reference/sanger-variant-calling/qcGenotype_GRCh38_hla_decoy_ebv.tar.gz".format(work_dir)
            },
            "repackSangerResults": {
                "cpus": 2,
                "mem": 4
            },
            "repack-sanger-results": {
                "cpus": 2,
                "mem": 4
            },
            "prepSangerSupplement": {
                "cpus": 2,
                "mem": 8
            },
            "prep-sanger-supplement": {
                "cpus": 2,
                "mem": 8
            },
            "cavemanVcfFix": {
                "cpus": 2,
                "mem": 16
            },
            "caveman-vcf-fix": {
                "cpus": 2,
                "mem": 16
            },
            "extractSangerCall": {
                "cpus": 2,
                "mem": 4
            },
            "extract-sanger-call": {
                "cpus": 2,
                "mem": 4
            },
            "payloadGenVariantCall": {
                "cpus": 2,
                "mem": 8
            },
            "payload-gen-variant-call": {
                "cpus": 2,
                "mem": 8
            },
            "prepSangerQc": {
                "cpus": 2,
                "mem": 8
            },
            "prep-sanger-qc": {
                "cpus": 2,
                "mem": 8
            },
            "upload": {
                "song_cpus": 2,
                "song_mem": 2,
                "score_cpus": 8,
                "score_mem": 18
            }
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
        return "{}-{}".format(self.wp_config["normal_aln_analysis_id"], self.wp_config["tumour_aln_analysis_id"])
