from service.request_models.WorkflowRequest import WorkflowRequest

class AlignRequest(WorkflowRequest):
    def __init__(self, workflow_url, config=None):
        super().__init__(workflow_url, config)

    def buildParams(self, config):
        return {
            "test": "params"
        }
