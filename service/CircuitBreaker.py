import dateutil.relativedelta
from gql import gql
from datetime import datetime
from service.Wes import Wes

class CircuitBreaker:
    def __init__(self, limit, range_in_days):
        self.limit = limit
        self.range_in_days = range_in_days
        self.is_blown = False
        self.error_count = 0
        
        self.update()

    @property
    def error_count(self):
        return self.__error_count

    @property
    def is_blown(self):
        return self.__is_blown

    @error_count.setter
    def error_count(self, error_count):
        self.__error_count = error_count

    @is_blown.setter
    def is_blown(self, is_blown):
        self.__is_blown = is_blown

    def update(self):
        query = gql('''
        {
            runs(page: {from: 0, size: 10}, filter: {state: "EXECUTOR_ERROR"}) {
                runName
                state
                completeTime
            }
        }
        ''')

        error_runs = Wes.fetchWesRunsAsDataframeForWorkflow(query, self.__transformData)
        num_overloads = error_runs["overload"].sum()

        self.error_count = num_overloads
        self.is_blown = num_overloads >= self.limit

    def __transformData(self, data):
        now = datetime.now()
        completeTime = datetime.fromtimestamp(int(data["completeTime"]) / 1000)
        delta = dateutil.relativedelta.relativedelta(now, completeTime)

        return {
            "run_id": data["runName"],
            "state": data["state"],
            "completeTime": format(completeTime),
            "overload": True if delta.days < self.range_in_days else False
        }

