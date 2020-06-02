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
            cb.update()
    
            if not cb.is_blown:
                method_to_call(**kwargs)
            else:
                print("Fuse Blown!")
                print("Error count: ".format(cb.error_count))
                wf.update()
        return func
