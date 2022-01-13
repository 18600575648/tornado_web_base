import time
from typing import Any
import uuid
from collections import OrderedDict, defaultdict
from logging import Logger
from tornado.escape import url_escape, json_decode

import tornado.auth
import ujson as json
from tornado.log import app_log
from tornado.web import RequestHandler
from components.utils.log import ExtraLog

def guid():
    return str(uuid.uuid4()).replace('-', '')


def strftime_ms(timestamp):
    str = '%s.%d' % (time.strftime('%Y-%m-%d %H:%M:%S',
                     time.localtime(timestamp)), (timestamp * 1000) % 1000)
    return str

class DefaultHandler(RequestHandler):

    def initialize(self):
        super().initialize()
        self.log = ExtraLog(self, app_log)
        # convert request.header to defaultdict
        self.headers = defaultdict(str, self.request.headers)

        old_request_id = self.headers['request_id'].strip()

        self.request_id = self.headers['request_id'] = guid().strip()
        self.headers['request_trace'] = self.request_trace = ' '.join(
            filter(
                lambda x: x.strip(),
                [
                    self.headers['request_trace'],
                    '' if old_request_id in self.headers['request_trace'] else old_request_id,
                    self.headers['request_id']
                ])).strip()

        content_type = self.headers['Content-Type']
        if content_type in ('application/x-json', 'application/json'):
            self.body_json = tornado.escape.json_decode(self.request.body)
        else:
            self.body_json = None

        self.query_arguments = defaultdict(str, { key : self.get_query_argument(key,'') for key in self.request.query_arguments.keys()})
        self.body_arguments = defaultdict(str, { key : self.get_body_argument(key,'') for key in self.request.body_arguments.keys()})
        self.arguments = defaultdict(list, { key : self.get_argument(key,'') for key in self.request.arguments.keys()})
        # the first time call set_default_header is earlier than initialize, call it again
        self.set_default_headers()
        return

    # Called at the beginning of a request before get/post/etc.
    async def prepare(self):

        return super().prepare()

    def on_finish(self):  # Called after the end of a request, the connection is closed, do any housekeeping here
        super().on_finish()
        return

    def get_current_user(self) -> Any:
        user = self.get_secure_cookie("user")
        if user:
            return user
        return super().get_current_user()

    def set_default_headers(self) -> None:
        super().set_default_headers()        
        if 'request_id' in self.__dict__:
            self.set_header("request_id", self.request_id)
            self.set_header("request_trace", self.request_trace)
        return

    # customize the output content
    def _request_summary(self) -> str:
        body = self.body_json if self.body_json else self.body_arguments
        if body is dict:
            body = {key: url_escape(value) for (key, value) in body.items() if not key in (
                'requestid', 'request_id', 'request_trace',)}

        request_info = OrderedDict({
            'remote_ip': self.request.remote_ip,
            'host': self.request.host,
            'port': self.settings['port'],
            'request_id': self.request_id,
            'request_trace': self.request_trace,
            'start_time': strftime_ms(self.request._start_time),
            'finish_time': strftime_ms(self.request._start_time + self.request.request_time()),
            'request_time (s)': round(self.request.request_time(), 3),
            'content_info': {
                'type': 'web_api_test request_start request_summary',
                'name': '%s.%s' % (self.__module__, self.__class__.__name__),
                'response': [],
                'request': {
                    'host': self.request.host,
                    'status': self.get_status(),
                    'url': "http://{{url}}"+self.request.uri,
                    'method': self.request.method,
                    'body_info': {'mode': 'raw', 'raw': body},
                    'header': [{'key': key, 'value': self.request.headers[key]} for key in self.request.headers if not key.startswith('X-') and key != 'Content-Type']
                    + [
                        {'key': 'Content-Type', 'value': 'application/json'},
                        {'key': 'Api-test-mode', 'value': 'true'}
                    ],
                }
            }
        })

        try:
            return json.dumps(request_info, indent=True)
        except Exception as e:
            return e
        
    def write(self, data) -> None:        
        super().write(json.dumps(data, indent=True) if data != None and type(data) == list else data)
            

    # customize the exception fallover handler
    def _handle_request_exception(self, e: BaseException) -> None:
        return super()._handle_request_exception(e)

# 不同类型的handler，缺省值不同
class UIHandler(DefaultHandler):
    def set_default_headers(self) -> None:
        super().set_default_headers()
        self.set_header("Content-Type", "text/html;charset=utf-8")        
        return 

class ServiceHandler(DefaultHandler):
    def set_default_headers(self) -> None:
        super().set_default_headers()
        self.set_header("Content-Type", "application/json;charset=utf-8")
        return 