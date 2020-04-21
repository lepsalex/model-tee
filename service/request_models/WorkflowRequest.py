from abc import ABC, abstractmethod


class WorkflowRequest(ABC):
    def __init__(self, workflow_url, config=None):
        self.workflow_url = workflow_url
        self.wp_config = self.buildParams(config)
        self.wep_config = self.__buildEngineParams(config)

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
    def buildParams(self, config):
        pass

    def __buildEngineParams(self, config):
        # return None of not specified
        if config == None:
            return None

        engine_params = {}

        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "launch_dir", "/{}/launch", config.get("work_dir", None))
        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "project_dir", "/{}/project", config.get("work_dir", None))
        WorkflowRequest.addFormattedStringValueIfValue(engine_params, "work_dir", "/{}/work", config.get("work_dir", None))

        WorkflowRequest.addValueIfValue(engine_params, "revision", config.get("revision", None))

        return engine_params

    @staticmethod
    def addValueIfValue(dict, key, val):
        if not val:
            return

        dict[key] = val

    @staticmethod
    def addFormattedStringValueIfValue(dict, key, formatted, val):
        if not val:
            return

        dict[key] = formatted.format(val)
