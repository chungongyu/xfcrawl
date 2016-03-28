# -*- coding: utf-8 -*-

import base64
import json
import logging
import string
import time

import pika
from redis import Redis
import tld

import utils

_logger = logging.getLogger('dispatcher')

def consume_isvalid(**parms):
    if not parms.has_key('id'): return False
    if not parms.has_key('data') or not parms['data'].has_key('links'): return False
    return True

def consume_filter(options, **parms):
    def _filter_js(options, links):
        urls = []

        for url in links:
            if not url.startswith('javascript:'):
                urls.append(url)

        return urls

    def _filter_site(options, links):
        urls = []

        def _get_tld(url):
            try: return utils.utf8(tld.get_tld(url))
            except: return None

        redisdb = options['redis']

        links = map(lambda x: (x, _get_tld(x)), links)
        sites = set(filter(
                        lambda x: x is not None, 
                        map(lambda x: x[1], links)
                    )
                )
        site_states = redisdb.smembers('sites_allowed') & sites

        for url, site in links:
            if site in site_states:
                urls.append(url)

        return urls
    def _filter_state(options, links):
        urls = []

        redisdb = options['redis']
        now = time.time()

        url_states = redisdb.mget(links) if len(links) > 0 else []
        with redisdb.pipeline() as pipe:
            for i, url in enumerate(links):
                if url_states[i] is None:
                    urls.append(url)
                    pipe.setex(url, json.dumps({'dispatch_time': now}), options.get('urlstate.expire', 24*3600))
            pipe.execute()
        return urls

    if consume_isvalid(**parms):
        links = map(lambda x: string.strip(utils.utf8(x)), parms['data']['links'])

        for _filter in [_filter_js, _filter_site, _filter_state]:
            links = list(_filter(options, links))
        
        return links

    return []

def consume_callback(channel, method, properties, body, options):
    _logger.info(body)

    try:
        parms = json.loads(body)

        # filter the urls
        data = {'id': parms['id'], 'wget': parms.get('wget', {})}
        urls = consume_filter(options, **parms)
        for url in urls:
            _logger.info(url)

            data['wget']['url'] = url
            ###########################
            # send it to extractor
            ###########################
            channel.basic_publish(exchange='', 
                    routing_key=options['client.queue'][0], 
                    body = json.dumps(data)
                    )
    except Exception, e:
        _logger.error('%s' % (str(e)))

def consume_run(options):
    options['redis'] = Redis.from_url(options['redis'], db=1)

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
            'worker.queue': 'letv.spider.dispatcher', 
            'client.queue': ('letv.spider.downloader',), 
            'redis': 'redis://localhost:6379', 
            'urlstate.expire': 3600*24, 
            'log.config': os.path.join(os.path.dirname(__file__), 'config', 'log4j.properties')
            }

    opts, args = getopt.getopt(sys.argv[1:], 's:r:c:h', ['server=', 'redis=', 'log.config=', 'help'])
    for o, a in opts:
        if o == '-s' or o == '--server':
            options['server'] = a
        if o == '-d' or o == '--redis':
            options['redis'] = a
        elif o == '-c' or o == '--log.config':
            options['log.config'] = a
        elif o == '-h' or o == '--help':
            print '%s -s <server> -r <redis> -c <log.config>' % (sys.argv[0])
            sys.exit(0)

    # config logging
    logging.config.fileConfig(options['log.config'])

    consume_run(options)
