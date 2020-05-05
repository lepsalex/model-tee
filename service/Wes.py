import os
import json
import requests
import aiohttp
import asyncio
import pandas as pd
from gql import Client
from gql.transport.requests import RequestsHTTPTransport
from dotenv import load_dotenv

load_dotenv(".env.dev")


class Wes:

    _transport = RequestsHTTPTransport(
        url=os.getenv("WES_GQL"),
        use_json=True,
    )

    client = Client(
        transport=_transport,
        fetch_schema_from_transport=True,
    )

    @classmethod
    def fetchWesRunsAsDataframeForWorkflow(cls, query, transform_func):
        # init to empty dataframe
        runs_df = pd.DataFrame()

        try:
            # execute gql query
            response = cls.client.execute(query)

            # convert gql response to something we can work with
            runs = [transform_func(run) for run in response["runs"]]

            # create new dataframe and reassign
            runs_df = pd.DataFrame(runs)
        except ValueError as err:
            # log error and return empty dataframe
            print(err)

        return runs_df

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
