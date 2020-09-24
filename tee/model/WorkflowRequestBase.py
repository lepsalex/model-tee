import os
from abc import ABC, abstractmethod


class WorkflowRequestBase(ABC):
    def __init__(self, workflow_url, run_config=None):
        self.song_score_config = {
            "SONG_URL": os.getenv("SONG_URL"),
            "SCORE_URL": os.getenv("SCORE_URL"),
            "INTERMEDIATE_SONG_URL": os.getenv("INTERMEDIATE_SONG_URL"),
            "ICGC_SCORE_URL": os.getenv("ICGC_SCORE_URL"),
            "ICGC_SCORE_TOKEN": os.getenv("ICGC_SCORE_API_TOKEN")
        }

        self.workflow_url = workflow_url
        self.wp_config = self.buildWorkflowParams(run_config, self.song_score_config)
        self.wep_config = WorkflowRequestBase.buildEngineParams(run_config)

    def data(self):
        """
        Returns WorkflowRequest in WES format as Dict
        """
        data = {
            "workflow_url": self.workflow_url
        }

        WorkflowRequestBase.addValueIfValue(data, "workflow_params", self.wp_config)
        WorkflowRequestBase.addValueIfValue(data, "workflow_engine_params", self.wep_config)

        return data

    @abstractmethod
    def buildWorkflowParams(self, run_config, song_score_config):
        pass

    @classmethod
    def buildEngineParams(cls, run_config):
        # return None of not specified
        if run_config == None:
            return None

        engine_params = {}

        WorkflowRequestBase.addFormattedStringValueIfValue(engine_params, "launch_dir", "/%s/launch", run_config.get("work_dir", None))
        WorkflowRequestBase.addFormattedStringValueIfValue(engine_params, "project_dir", "/%s/versioned-projects/%s/", run_config.get("work_dir", None), run_config.get("revision", "master"))
        WorkflowRequestBase.addFormattedStringValueIfValue(engine_params, "work_dir", "/%s/work", run_config.get("work_dir", None))

        WorkflowRequestBase.addValueIfValue(engine_params, "revision", run_config.get("revision", "master"))
        WorkflowRequestBase.addValueIfValue(engine_params, "resume", run_config.get("resume", None))

        return engine_params

    @classmethod
    def addValueIfValue(cls, dict, key, val):
        if not val:
            return

        dict[key] = val

    @classmethod
    def addFormattedStringValueIfValue(cls, dict, key, formatted, *val):
        if None in val:
            return

        dict[key] = formatted % tuple(val)
