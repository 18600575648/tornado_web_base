#!/usr/bin/ python
# -*- coding: utf-8 -*-

import logging
from collections import defaultdict

import tornado.httpserver
import tornado.ioloop
import tornado.log
import tornado.web
from tornado.options import define, options
from tornado.web import Application, RequestHandler

from components.basehandler.webapp import (AppLogger, IPAApplication,
                                           LogFormatter)
from components.utils.misc import createIfNotExists

SERVER_CONFIG = "./config/server_config.py"

def make_app():
    
    logging.setLoggerClass(AppLogger)    
    from config.server_config import define_options
    [define(opt,default,type,help) for opt,default,type,help in define_options]
    
    options.parse_config_file(SERVER_CONFIG)    
    # remove: this will call tornado.log.enable_pretty_logging twice and create duplicate handlers
    # options.parse_command_line()    # command line own the top priority
    [i.setFormatter(LogFormatter()) for i in logging.getLogger().handlers]

    handler_map = []
    # add more handler file here
    from config.handlers import handler_list
    for handler in handler_list:
        exec(f'from {handler} import handler_map as handler_entry')
        exec(f'handler_map += handler_entry')

    app = IPAApplication(handler_map, **options.as_dict())

    # change to defaultdict, much more easier latter
    def NotExist():
        return None
    app.settings = defaultdict(NotExist, app.settings)
    return app


#############################################################################
if __name__ == "__main__":
    app = make_app()
    server = tornado.httpserver.HTTPServer(app)
    server.listen(app.settings.get('port', 80),
                  address=app.settings.get('address', ''))
    server.start(app.settings.get('forks', 1))  # forks one process per cpu
    
    io = tornado.ioloop.IOLoop.current()
    io.add_callback(app.execute_start_command)    
    io.start()
