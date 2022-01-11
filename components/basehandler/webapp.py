#!/usr/bin/ python
# -*- coding: utf-8 -*-

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, List, Optional
from tornado import httpclient

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web
from apscheduler.schedulers.tornado import TornadoScheduler
from components.webservice.helloworld.handler import ExtraLog
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.log import access_log
from tornado.process import Subprocess
from tornado.web import Application, RequestHandler

from config.start_command import start_command


class IPAApplication(tornado.web.Application):
    # scheduler = None
    # job_ids   = []
    # def __init__(self,**args):        
    #     global scheduler
    #     if not scheduler:
    #         scheduler = TornadoScheduler()
    #         scheduler.start()    
    #         logging.getLogger().info('[Scheduler Init]APScheduler has been started')
    #         self.scheduler = scheduler

    def log_request(self, handler: RequestHandler) -> None:
        """Writes a completed HTTP request to the logs.

        By default writes to the python root logger.  To change
        this behavior either subclass Application and override this method,
        or pass a function in the application settings dictionary as
        ``log_function``.
        """
        log = ExtraLog(handler, access_log)
        if "log_function" in self.settings:
            self.settings["log_function"](handler)
            return
        if handler.get_status() < 400:
            log_method = log.info
        elif handler.get_status() < 500:
            log_method = log.warning
        else:
            log_method = log.error
        log_method(handler._request_summary())

    async def run_command(self, command):    
        process = Subprocess(
            [command]
        )        

    async def execute_start_command(self, commands=[]):
        if not commands:
            commands = start_command
        if not commands:
            return        
        await asyncio.sleep(5)
        for item in commands:
            http_client = None
            try:
                if item.startswith('http'):
                    http_client = httpclient.AsyncHTTPClient()
                    response = await http_client.fetch(item)
                    logging.getLogger().info(f"execute {item}\n{response.body}")                    
                else:
                    await self.run_command(item)
            except Exception as ex:
                logging.getLogger().info(ex)
            finally:
                if http_client and not http_client._closed:
                    http_client.close()

class AppLogger(logging.Logger):

    def process(self, msg, kwargs):
        msg, kwargs = super().process(msg, kwargs)
        self.extra = kwargs.get('extra', {'request_trace': ''})
        if not 'request_trace' in self.extra:
            self.extra['request_trace'] = ''
        kwargs['extra'] = self.extra
        return msg, kwargs


class PageNotFoundHandler(RequestHandler):
    def get(self):
        self.write_error(404)


class LogFormatter(tornado.log.LogFormatter):

    def __init__(self):
        super(LogFormatter, self).__init__(
            fmt='''%(color)s[%(asctime)s.%(msecs)03d %(filename)s:%(funcName)s:%(lineno)d %(levelname)s %(request_trace)s]%(end_color)s %(message)s
''',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    def format(self, record: Any) -> str:
        if not 'request_trace' in record.__dict__:
            record.request_trace = ''
        str = ''
        try:
            str = super().format(record)
        except Exception as e:
            print(e)

        return str


handler_map = [(r'.*', PageNotFoundHandler)]
