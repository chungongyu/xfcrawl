# -*- coding: utf-8 -*-

import pymongo
from redis import Redis
import web

urls = (
        '/', 'index', 
        '/rate_limit', 'rate_limiter'
        )

class index:
    def GET(self):
        return 'Hello!'

class rate_limiter:
    def GET(self):
        return 'rate_limiter'

if __name__ == '__main__':
    app = web.application(urls, globals())
    app.run()
