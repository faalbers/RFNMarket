import requests
from lxml import html
import json as js
from bs4 import BeautifulSoup as bs
from datetime import datetime 
from ..utils import log
from . import const

class Request():
    def logResponse(self, verbose=None, verboseContent=None, verboseOpenHTML=None):
        if self.__response == None: return
        if verbose == None: verbose = self.__verbose
        if verboseContent == None: verboseContent = self.__verboseContent
        if verboseOpenHTML == None: verboseOpenHTML = self.__verboseOpenHTML
        if not verbose: return
        indent = ' '*2
        log.debug('\n*** REQUEST ***')
        log.debug('\nurl   : %s' % self.__response.request.url)
        log.debug('path url: %s' % self.__response.request.path_url)
        log.debug('method: %s' % self.__response.request.method)
        log.debug('\nheaders:')
        for name, value in self.__response.request.headers.items():
            log.debug('%s%s: %s' % (indent, name, value))
        cookieHeader = self.__response.request.headers.get('cookie')
        if cookieHeader != None:
            log.debug('\ncookies:')
            cookies = {}
            for cookieElem in cookieHeader.split('; '):
                elems = cookieElem.split('=')
                cookies[elems[0]] = elems[1]
            log.debug(cookies)

        if verboseContent:
            contentType = self.__response.request.headers.get('content-type')
            if contentType != None:
                log.debug('\ncontent:')
                contentElems = contentType.split(';')
                type = contentElems[0].strip()
                params = {}
                log.debug('content type  : %s' % type)
                if len(contentElems) > 1:
                    for param in contentElems[1:]:
                        paramElems = param.strip().split('=')
                        params[paramElems[0]] = paramElems[1].strip()
                log.debug('content params: %s\n' % params)
                if type == 'multipart/form-data':
                    log.debug('body: bytes')
                    log.debug(self.__response.request.body)
                elif type == 'application/x-www-form-urlencoded' or type == 'text/plain':
                    log.debug('body: text')
                    log.debug(self.__response.request.body)
                elif type == 'application/json':
                    log.debug('body: binary json')
                    log.debug(js.loads(self.__response.request.body.decode('utf-8')))
                else:
                    log.debug('unknow content type: %s' % type)
        log.debug("-" * 20)
        
        mytest = 'https://financialmodelingprep.com/api/v3/stock/list?apikey=5fnCoFYnujpfmldsHKPRKeLHWQCKBFLK'
        log.debug('\n*** RESPONSE ***')
        log.debug('\nurl   : %s' % self.__response.url)
        log.debug('\nurl   : %s' % mytest)
        log.debug('status_code: %s: %s' % (self.__response.status_code, const.STATUS_CODES[self.__response.status_code]['short']))
        for name, value in self.__response.headers.items():
            log.debug('%s%s: %s' % (indent, name, value))

        if len(self.__response.cookies):
            log.debug('\ncookies:')
        for cookie in self.__response.cookies:
            log.debug('%s%s:' % (indent, cookie.name))
            log.debug('%svalue: %s' % (indent*2, cookie.value))
            if cookie.expires != None:
                log.debug('%sexpires: %s' % (indent*2, datetime.fromtimestamp(cookie.expires)))
            if cookie.domain != None:
                log.debug('%sdomain: %s' % (indent*2, cookie.domain))
            if cookie.path != None:
                log.debug('%sspath: %s' % (indent*2, cookie.path))
            if cookie.secure != None:
                log.debug('%ssecure: %s' % (indent*2, cookie.secure))
            if cookie.has_nonstandard_attr(('HttpOnly')):
                log.debug('%sHttpOnly: True' % (indent*2))
            if cookie.has_nonstandard_attr(('SameSite')):
                log.debug('%sSameSite: %s' % (indent*2, cookie.get_nonstandard_attr(('SameSite'))))
        
        if verboseContent:
            contentType = self.__response.headers.get('content-type')
            if contentType != None:
                log.debug('\ncontent:')
                contentElems = contentType.split(';')
                type = contentElems[0]
                params = {}
                log.debug('content type  : %s' % type)
                if len(contentElems) > 1:
                    for param in contentElems[1:]:
                        paramElems = param.strip().split('=')
                        params[paramElems[0]] = paramElems[1].strip()
                log.debug('content params: %s\n' % params)
                if type == 'text/html':
                    if verboseOpenHTML:
                        html.open_in_browser(html.fromstring(self.__response.text))
                    else:
                        log.debug(bs(self.__response.text, features='lxml').prettify())
                elif type == 'application/json':
                    log.debug(self.__response.json())
                elif type == 'text/plain':
                    log.debug(self.__response.text)
                else:
                    log.debug('unknow content type: %s' % type)
        
        log.debug()
        log.debug("-" * 20)
    
    def __init__(self, params={}, headers={}, cookies={}, verbose=False, verboseContent=False, verboseOpenHTML=False):
        self.__session = requests.Session()
        # add persisting cookies, params and headers
        self.__session.cookies.update(cookies)
        self.__session.params.update(params)
        self.__session.headers.update(headers)
        self.__response = None
        self.__verbose = verbose
        self.__verboseContent = verboseContent
        self.__verboseOpenHTML = verboseOpenHTML

    def get(self, url=None,
            params=None, headers=None, cookies=None, auth=None, proxies=None, timeout=None, allow_redirects=False,
            verbose=None, verboseContent=None, verboseOpenHTML=None):
        self.__response = self.__session.get(url, 
                                             params=params, headers=headers, cookies=cookies, auth=auth,
                                             proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
        self.logResponse(verbose, verboseContent, verboseOpenHTML)
        return self.__response

    def post(self, url,
            params=None, headers=None, cookies=None, auth=None,
            data=None, files=None, json=None,
            verbose=None, verboseContent=None, verboseOpenHTML=None):
        self.__response = self.__session.post(url, params=params, headers=headers, cookies=cookies, auth=auth,
                                              data=data, files=files, json=json,)
        self.logResponse(verbose, verboseContent, verboseOpenHTML)


