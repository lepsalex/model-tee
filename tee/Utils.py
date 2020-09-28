import uuid
from functools import reduce
from collections import Counter

class Utils():
    @staticmethod
    def methodOrUpdateFactory(wf, method_name, cb):
        """
        Returns a function that updates Circuit Breaker,
        call the workflow method specified if possible,
        otherwise it just calls the update method
        """
        method_to_call = getattr(wf, method_name)

        def func(**kwargs):
            if wf.max_runs == 0:
                return

            cb.update()
    
            if not cb.is_blown:
                method_to_call(**kwargs)
            else:
                print("Fuse Blown!")
                print("Error count: ".format(cb.error_count))
                wf.update()
        return func

    @staticmethod
    def generateTestSongPubEvent():
        return {
            "analysisId": str(uuid.uuid4()),
            "study": "TEST-MT",
            "state": "PUBLISHED"
        }

    @staticmethod
    def mergeRunCountsFuncGen(*workflows):
        def func(thisWf):
            otherWfs = filter(lambda wf: wf.sheet_range != thisWf.sheet_range, workflows)
            return reduce(lambda acc, curr: acc + curr.run_count, otherWfs, 0)
        return func
    
    @staticmethod
    def mergeWorkDirsInUseFuncGen(*workflows):
        def func(thisWf):
            otherWfs = filter(lambda wf: wf.sheet_range != thisWf.sheet_range, workflows)
            return reduce(lambda acc, curr: acc + curr.work_dirs_in_use, otherWfs, Counter())
        return func
