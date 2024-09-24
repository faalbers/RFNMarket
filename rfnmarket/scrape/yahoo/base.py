from ...utils import storage, log
from datetime import datetime, timedelta
from ..request import Request
from . import const
from .. import const as apiconst
from time import sleep
from ratelimit import limits, sleep_and_retry

class Base():
    def requestCall(self, requestArgs):
        try:
            response = self.request.get(**requestArgs)
        except Exception:
            log.exception('requestCall')
            return None

        return response

    @sleep_and_retry
    @limits(calls=150, period=60)
    def requestCallLimited(self, requestArgs):
        return self.requestCall(requestArgs)

    def testValue(self):
        requestArgs = {
            'url': 'https://query2.finance.yahoo.com/v8/finance/chart/BBD',
            'params': {
                'range': '5y',
                'interval': '1d',
                'events': 'div,splits,capitalGains',
            },
            'timeout': 30,
        }
        log.debug('**** run test ****')
        response = self.requestCallLimited(requestArgs)
        if response != None:
            status_code = response.status_code
            if response.status_code == 200 and response.headers.get('content-type').startswith('application/json'):
                return len(response.json()['chart']['result'][0]['events']['dividends'])
        log.debug('test did not return valid value')
        return None

    def initRequest(self):
        yconfig = storage.get('database/yahoo')
        configRefresh = True
        if yconfig != None:
            # refresh cookie and crumb one month before it expires
            configRefresh = (datetime.now()+timedelta(days=30)).timestamp() > yconfig['cookie'].expires
        if configRefresh:
            req = Request(headers={'User-Agent': const.YAHOO_USER_AGENT})
            
            # get authorization cookie
            requestArgs = {
                'url': 'https://fc.yahoo.com',
                'timeout': 30,
                'proxies': None,
                'allow_redirects': True,
            }
            result = req.get(**requestArgs)
            cookie = list(result.cookies)
            if len(cookie) == 0:
                self.session = None
            cookie = cookie[0]

            # get authorization crumb
            requestArgs['url'] = 'https://query1.finance.yahoo.com/v1/test/getcrumb'
            result = req.get(**requestArgs)
            crumb = result.text
             
            # create config variable and save it
            yconfig = {'cookie': cookie, 'crumb': crumb}
            storage.save('database/yahoo', yconfig)

        # create session with auth cookie
        cookies = {yconfig['cookie'].name: yconfig['cookie'].value}
        params = {'crumb': yconfig['crumb']}
        headers = {'User-Agent': const.YAHOO_USER_AGENT}
        # self.request = Request(cookies=cookies, verbose=True, verboseContent=True,verboseOpenHTML=True)
        self.request = Request(params=params, cookies=cookies, headers=headers)

    def __init__(self):
        self.initRequest()

    def multiRequest(self, requestArgsList, blockSize=50, limited=True):
        retryReqArgsIndices = range(len(requestArgsList))
        sleepTime = 60
        # check test value to make sure data is consistent
        testValue = self.testValue()
        if testValue < 65:
            log.error('Initial test failed: No data returned')
            return None
        newTestValue = testValue
        while len(retryReqArgsIndices) != 0:
            reqArgsIndices = retryReqArgsIndices
            retryReqArgsIndices = []
            lastBlockReqArgsIndices = []
            reqArgsIndicesCount = len(reqArgsIndices)
            rangeCount = reqArgsIndicesCount / blockSize
            for x in range(int(rangeCount) + ((rangeCount) > int(rangeCount))):
                blockReqArgsIndices = reqArgsIndices[x*blockSize:(x+1)*blockSize]
                sleepTotal = 0
                while newTestValue < testValue:
                    log.debug('Test Failed: wait %s seconds and retry' % sleepTime)
                    sleepTotal += sleepTime
                    sleep(sleepTime)
                    newTestValue = self.testValue()
                if sleepTotal > 0:
                    log.debug('Test OK: Continued after %s seconds total wait' % sleepTotal)
                    for noneReqArgsIndex in lastBlockReqArgsIndices:
                        retryReqArgsIndices.append(noneReqArgsIndex)
                lastBlockReqArgsIndices = []
                statusCodes = {}
                log.debug('Still %s requests to do ...' % reqArgsIndicesCount)
                for reqArgsIndex in blockReqArgsIndices:
                    requestArgs = requestArgsList[reqArgsIndex]
                    if limited:
                        response = self.requestCallLimited(requestArgs)
                    else:
                        response = self.requestCall(requestArgs)
                    reqArgsIndicesCount -= 1
                    self.pushAPIData(reqArgsIndex, response)
                    if not response.status_code in statusCodes:
                        statusCodes[response.status_code] = 0
                    statusCodes[response.status_code] += 1
                    lastBlockReqArgsIndices.append(reqArgsIndex)
                for statusCode, scCount in statusCodes.items():
                    log.debug('got %s requests with status code: %s: %s' % (scCount, statusCode, apiconst.STATUS_CODES[statusCode]['short']))
                # check test value after each block to make sure data is consistent
                newTestValue = self.testValue()
            # we commit after blocksize entry
            self.dbCommit()
            if len(retryReqArgsIndices) > 0:
                log.debug('Retrying %s more requests that did not pass the test ...' % len(retryReqArgsIndices))
