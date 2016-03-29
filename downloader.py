# -*- coding: utf-8 -*-

import base64
import json
import logging
import time
import urlparse

from expiringdict import ExpiringDict
import pika
from redis import Redis
import requests

import utils

_logger = logging.getLogger('downloader')


class RateLimiter:
    def __init__(self, redis, window, limit, delta=0, namespace='RateLimiter'):
        self._redis = redis;
        self._window = window * 1000
        self._limit = limit
        self._delta = delta * 1000
        self._namespace = namespace

    def limit(self, channel, userId, item):
        now = int(time.time() * 1000)
        key = '%s-%s-%s' % (self._namespace, channel, userId)
        discard = now - self._window

        with self._redis.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    r = pipe.zrangebyscore(key, discard, now, withscores=True)
                    if r:
                        if len(r) >= self._limit:
                            return int(r[0][1]) - now + self._window
                        elif self._delta > 0 and self._delta > now - int(r[-1][1]):
                            return self._delta - (now - int(r[-1][1]))
                    pipe.multi()
                    pipe.zremrangebyscore(key, 0, discard - 1)
                    pipe.zadd(key, item, now)
                    pipe.expire(key, self._window / 1000)
                    pipe.execute()
                    break
                except WatchError, e:
                    pass

        return 0

def consume_isvalid(**parms):
    if not parms.has_key('url'): return False
    method = parms.get('method', 'get')
    if method != 'get' and method != 'post': return False
    return True

def consume_get_host(url):
    parts = urlparse.urlparse(url)
    return parts.netloc

def consume_ratelimit(options, url):
    try:
        redisdb = options['redis']
        host = consume_get_host(url)
        parms = redisdb.hget('rate-limit', host)
        if parms:
            parms = json.loads(parms)
            r = RateLimiter(redisdb, parms['window'], parms['limit'], parms['delta'])
            while True:
                wait = r.limit(host, options['user.id'], url)
                if wait == 0:
                    break
                time.sleep(wait / 1000.0)
    except Exceptin, e:
        _logger.error('RateLimit: %s' % str(e))

def consume_request(options, **parms):
    wget = parms.get('wget', {})

    if consume_isvalid(**wget):
        # wget parameters
        url, method = wget['url'], wget.get('method', 'get')
        headers, cookies = wget.get('headers', {}), wget.get('cookies', {})
        fields = wget.get('fields')

        if not headers.has_key('user-agent') and options.has_key('wget.user-agent'):
            headers['user-agent'] = options['wget.user-agent']

        # get token
        consume_ratelimit(options, wget['url']);

        try:
            if method == 'get':
                return requests.get(url, timeout=options.get('wget.timeout', 3600), params=fields, headers=headers, cookies=cookies)
            elif method == 'post':
                return requests.post(url, timeout=options.get('wget.timeout', 3600), data=fields, headers=headers, cookies=cookies)
        except Exception, e:
            _logger.exception('url: %s get page fail' % url)

    return None

def consume_callback(channel, method, properties, body, options):
    _logger.info(body)

    try:
        parms = json.loads(body)
        # wget the url
        r = consume_request(options, **parms)
        if r:
            data = base64.b64encode(r.content)
            parms['data'] = {'page': data, 'status': r.status_code}
            ###########################
            # send it to extractor
            ###########################
            channel.basic_publish(exchange='', 
                    routing_key=options['client.queue'][0], 
                    body = json.dumps(parms)
                    )
        else:
            _logger.error(body)
    except Exception, e:
        _logger.error('%s' % (str(e)))

def consume_run(options):
    options['redis'] = Redis.from_url(options['redis'], db=0)

    connection = pika.BlockingConnection(pika.URLParameters(options['server']))
    channel = connection.channel()

    channel.queue_declare(queue=options['worker.queue'])
    for queue in options['client.queue']:
        channel.queue_declare(queue=queue)

    channel.basic_consume(
            lambda channel, method, properties, body: consume_callback(channel, method, properties, body, options), 
            queue=options['worker.queue'], 
            no_ack=True
            )

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    import os,sys
    import getopt
    import logging.config

    options = {
            'server': 'amqp://localhost:5672', 
            'worker.queue': 'letv.spider.downloader', 
            'client.queue': ('letv.spider.extractor',), 
            'redis': 'redis://localhost:6379', 
            'wget.user-agent': 'letv.spider', 
            'wget.timeout': 15, 
            'user.id': 'default', 
            'log.config': os.path.join(os.path.dirname(__file__), 'config', 'log4j.properties')
            }

    opts, args = getopt.getopt(sys.argv[1:], 's:r:A:u:c:h', ['server=', 'redis=', 'user-agent=', 'timeout=', 'user.id=', 'log.config=', 'help'])
    for o, a in opts:
        if o == '-s' or o == '--server':
            options['server'] = a
        if o == '-r' or o == '--redis':
            options['redis'] = a
        if o == '-A' or o == '--user-agent':
            options['wget.user-agent'] = a
        if o == '--timeout':
            options['wget.timeout'] = int(a)
        if o == '-u' or o == '--user.id':
            options['user.id'] = a
        elif o == '-c' or o == '--log.config':
            options['log.config'] = a
        elif o == '-h' or o == '--help':
            print '%s -s <server> -r <redis> -A <user-agent> --timeout <timeout> -u <user.id> -c <log.config>' % (sys.argv[0])
            sys.exit(0)

    # config logging
    logging.config.fileConfig(options['log.config'])

    consume_run(options)
