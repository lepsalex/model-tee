import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd

class Wes:
    base_url = os.getenv("WES_BASE")

    @classmethod
    def fetchWesRunsAsDataframeForWorkflow(cls, workflow_url, transform_func):
        try:
            # get runIds from WES (can throw ValueError on no runs)
            runIds = cls.fetchWesRunIds()

            # get data for runs belonging to this workflow
            loop = asyncio.get_event_loop()
            coroutines = [cls.fetchWesRunForWf(runId, workflow_url, transform_func) for runId in runIds]
            runs = [run for run in loop.run_until_complete(asyncio.gather(*coroutines)) if run]

            return pd.DataFrame(runs)
        except ValueError as err:
            # log error and return empty dataframe
            print(err)
            return pd.DataFrame()

    @classmethod
    def fetchWesRunIds(cls):
        data = requests.get(cls.base_url).json()
        run_ids = [run["run_id"] for run in data["runs"]]

        if len(run_ids) == 0:
            raise ValueError("No runs exist in WES!")

        return run_ids

    @classmethod
    async def fetchWesRunForWf(cls, wesId, workflow_url, transform_func):
        """
        Returns a run only if it is for this workflow,
        otherwise returns False
        """
        async with aiohttp.ClientSession() as session:
            async with session.get("{}/{}".format(os.getenv("WES_BASE"), wesId.strip())) as response:
                data = await response.json()

                if data["request"]["workflow_url"] == workflow_url:
                    return transform_func(data)
                else:
                    return False
