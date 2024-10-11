from rauth import OAuth1Service
from database.keys import KEYS
import webbrowser
from ..request import Request
from ...utils import log
from ratelimit import limits, sleep_and_retry
from .. import const as apiconst
from pprint import pp

class Base():
    def requestCall(self, requestArgs):
        try:
            response = self.request.get(requestArgs)
        except Exception:
            log.exception('requestCall')
            return None
        return response

    @sleep_and_retry
    @limits(calls=4, period=1)
    def requestCallLimited(self, requestArgs):
        return self.requestCall(requestArgs)

    def initRequest(self):
        etrade = OAuth1Service(
            name="etrade",
            consumer_key=KEYS['ETRADE']['KEY'],
            consumer_secret=KEYS['ETRADE']['SECRET'],
            request_token_url="https://api.etrade.com/oauth/request_token",
            access_token_url="https://api.etrade.com/oauth/access_token",
            authorize_url="https://us.etrade.com/e/t/etws/authorize?key={}&token={}",
            base_url='https://api.etrade.com')
        
        request_token, request_token_secret = etrade.get_request_token(
            params={"oauth_callback": "oob", "format": "json"})

        authorize_url = etrade.authorize_url.format(etrade.consumer_key, request_token)
        webbrowser.open(authorize_url)
        text_code = input("Please accept agreement and enter text code from browser: ")

        self._session = etrade.get_auth_session(request_token,
                                        request_token_secret,
                                        params={"oauth_verifier": text_code})
        # self.request = Request(session=self._session, verbose=True, verboseContent=True,verboseOpenHTML=True)
        self.request = Request(session=self._session)

    def __init__(self):
        self.initRequest()


    def __del__(self):
        url = 'https://api.etrade.com/oauth/revoke_access_token'
        try:
            self._session.get(url)
            log.info('etrade: revoke access')
        except:
            log.info('etrade: revoke access failed')
