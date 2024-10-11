from database.keys import KEYS
from ..request import Request
from ...utils import log
from ratelimit import limits, sleep_and_retry

class Base():
    def requestCall(self, requestArgs):
        try:
            response = self.request.get(requestArgs)
        except Exception:
            log.exception('requestCall')
            return None
        return response

    # upped period a bit from 60 because i was getting error You've exceeded the maximum requests per minute
    @sleep_and_retry
    @limits(calls=5, period=70)
    def requestCallLimited(self, requestArgs):
        return self.requestCall(requestArgs)

    def initRequest(self):
        self.request = Request(params = {'apikey': KEYS['POLYGON']['KEY']})

    def __init__(self):
        self.initRequest()
