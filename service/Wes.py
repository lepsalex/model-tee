import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd


class Wes:

    @classmethod
    def fetchWesRunsAsDataframeForWorkflow(cls, workflow_url, transform_func):
        # init to empty dataframe
        runs_df = pd.DataFrame()

        try:
            # get runIds from WES (can throw ValueError on no runs)
            runIds = cls.fetchWesRunIds()

            # get data for runs belonging to this workflow
            loop = asyncio.get_event_loop()
            coroutines = [cls.fetchWesRunForWf(runId, workflow_url, transform_func) for runId in runIds]
            runs = [run for run in loop.run_until_complete(asyncio.gather(*coroutines)) if run]

            # create new dataframe and reassign
            runs_df = pd.DataFrame(runs)
        except ValueError as err:
            # log error and return empty dataframe
            print(err)

        return runs_df

    @classmethod
    def fetchWesRunIds(cls):
        data = requests.get(os.getenv("WES_GQL")).json()
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
            async with session.get("{}/{}".format(os.getenv("WES_GQL"), wesId.strip())) as response:
                data = {}

                try:
                    data = await response.json()
                except:
                    print("Request failed for runId: ", wesId.strip())

                # in the event that a run_id doesn't have real data (buggy)
                if data.get("request", None) is None:
                    return False

                # return only data for workflow we're interested in
                if data["request"]["workflow_url"] == workflow_url:
                    return transform_func(data)
                else:
                    return False

    @classmethod
    def startWesRuns(cls, run_requests):
        loop = asyncio.get_event_loop()
        coroutines = [cls.starWesRun(run_request) for run_request in run_requests]
        return loop.run_until_complete(asyncio.gather(*coroutines))

    @classmethod
    async def starWesRun(cls, run_request, semaphore=asyncio.Semaphore(1)):
        # start runs one at a time for now (Semaphore)
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                print("Starting new job for analysisId: ", run_request)

                async with session.post(os.getenv("WES_RUNS_BASE"), json=run_request.data()) as response:
                    data = await response.json()
                    print("New run started with runId: ", data["run_id"])
                    # return format for easy write into sheets as column
                    return [data["run_id"]]
