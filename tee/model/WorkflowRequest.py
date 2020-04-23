import os
from abc import ABC, abstractmethod


class WorkflowRequest(ABC):
    def __init__(self, workflow_url, run_config=None):
        self.song_score_config = {
            "SONG_URL": os.getenv("SONG_URL"),
            "SCORE_URL": os.getenv("SCORE_URL"),
            "INTERMEDIATE_SONG_URL": os.getenv("INTERMEDIATE_SONG_URL"),
            "ICGC_SCORE_URL": os.getenv("ICGC_SCORE_URL"),
            "SONG_API_TOKEN": os.getenv("SONG_API_TOKEN"),
            "ICGC_SCORE_TOKEN": os.getenv("ICGC_SCORE_API_TOKEN")
        }

        self.workflow_url = workflow_url
        self.wp_config = self.buildWorkflowParams(run_config, self.song_score_config)
        self.wep_config = WorkflowRequest.buildEngineParams(run_config)

    def data(self):
        """
        Returns WorkflowRequest in WES format as Dict
        """
        data = {
            "workflow_url": self.workflow_url
        }

        WorkflowRequest.addValueIfValue(data, "workflow_params", self.wp_config)
        WorkflowRequest.addValueIfValue(data, "workflow_engine_params", self.wep_config)

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

        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "launch_dir", "/{}/launch", run_config.get("work_dir", None))
        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "project_dir", "/{}/project", run_config.get("work_dir", None))
        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "work_dir", "/{}/work", run_config.get("work_dir", None))

        WorkflowRequest.addValueIfValue(engine_params, "revision", run_config.get("revision", None))
        WorkflowRequest.addValueIfValue(engine_params, "resume", run_config.get("resume", None))

        return engine_params

    @classmethod
    def addValueIfValue(cls, dict, key, val):
        if not val:
            return

        dict[key] = val

    @classmethod
    def addFormattedStringValueIfValue(cls, dict, key, formatted, val):
        if not val:
            return

        dict[key] = formatted.format(val)
