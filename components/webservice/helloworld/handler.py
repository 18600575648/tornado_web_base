from collections import defaultdict, OrderedDict
from logging import Logger
import ujson as json
import tornado.auth
from tornado.web import RequestHandler
from tornado import httpclient
from components.basehandler.basehandler import *
from components.database.mysqldb import MySqlDB
from components.utils.misc import guid

class HelloHandler(UIHandler):

    # add more http verb
    SUPPORTED_METHODS = RequestHandler.SUPPORTED_METHODS + ('PROPFIND',)

    def propfind(self):
        pass

    # http actions: get/head/post/delete/patch/put/options
    @tornado.web.authenticated
    def get(self):
        self.log.debug("Hello, world2")
        self.write("Hello, world2")

    def post(self):
        self.write("Hello, world2")

class DBHandler(UIHandler):

    # http actions: get/head/post/delete/patch/put/options
    async def get(self):
        try:
            db = MySqlDB(self.application.settings['mysql_config'])            
            await db.exec_sql('create database if not exists linku_ems')
            await db.exec_sql("""
            use linku_ems; 
            drop table if exists tblaccount;
            create table if not exists tblaccount(
                oid int unsigned auto_increment, 
                guid varchar(64) not null unique, 
                account_name varchar(64) not null,
                PRIMARY KEY (oid))
            """)
            await db.exec_sql(f'insert into tblaccount(guid,account_name)values("{guid()}","abc")')
            self.write(await db.exec_select('select guid from tblaccount order by oid limit 10'))
        except Exception as ex:
            self.log.exception(ex)
            self.write(ex)        

class SpiderHandler(DefaultHandler):
    @tornado.web.authenticated
    async def get(self):
        # arguement value is a list
        url = self.query_arguments['url']
        if not url or not url[0]:
            self.write('没有指定url')
            return
        http_client = httpclient.AsyncHTTPClient()
        try:
            response = await http_client.fetch(('https://' if url[0].startswith('http') else '') + url[0])
        except Exception as e:
            print("Error: %s" % e)
            self.write_error(400, **{"error": e.args})
        else:
            self.write(response.body)


class RedirectHandler(DefaultHandler):
    @tornado.web.authenticated
    async def get(self, url):
        if not url:
            self.write('没有指定url')
            return
        self.redirect(('https://' if not url.startswith('http') else '') + url)


class OpenAuthHandler(DefaultHandler, tornado.auth.GoogleOAuth2Mixin):
    async def get(self, service):
        if service == 'google':
            await self.google()
        return

    async def google(self):
        if self.arguments['code']:
            user = await self.get_authenticated_user(
                redirect_uri='http://your.site.com/auth/google',
                code=self.arguments['code'])
            # Save the user with e.g. set_secure_cookie
        else:
            self.authorize_redirect(
                redirect_uri='http://your.site.com/auth/google',
                client_id=self.settings['google_oauth']['key'],
                scope=['profile', 'email'],
                response_type='code',
                extra_params={'approval_prompt': 'auto'})


handler_map = [
    (r'/hello', HelloHandler),
    (r'/spider', SpiderHandler),
    #(r'/redirect/([\w\.]+)', RedirectHandler ),
    (r'/redirect/(?P<url>.+)', RedirectHandler),
    (r'/auth/(?P<service>.+)', OpenAuthHandler), 
    (r'/db', DBHandler), 
]
