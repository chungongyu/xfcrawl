# -*- coding: utf-8 -*-

import json
import logging

import pika
import pymongo

_logger = logging.getLogger('pipeline')

def consume_isvalid(**parms):
    if not parms.has_key('wget'):
        return False
    if not parms['wget'].has_key('url') or not parms['wget']['url']:
        return False
    if not parms.has_key('result'):
        return False

    return True

def consume_callback(channel, method, properties, body, options):
    _logger.info(body)
    try:
        data = json.loads(body)
        table = options['mongodb'].spider.datum
        if consume_isvalid(**data):
            url = data['wget']['url']
            table.update_one({"wget.url": url}, {"$set":data}, upsert=True)
        else:
            _logger.error(body)

    except Exception, e:
        _logger.error(str(e))

def consume_run(options):
    mongodb = pymongo.MongoClient(options['mongodb'])
    options['mongodb'] = mongodb

    connection = pika.BlockingConnection(pika.URLParameters(options['server']))
    channel = connection.channel()

    channel.queue_declare(queue=options['worker.queue'])

    channel.basic_consume(
            lambda channel, method, properties, body: consume_callback(channel, method, properties, body, options), 
            queue=options['worker.queue'], 
            no_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    import os,sys
    import getopt
    import logging.config

    options = {
            'server': 'amqp://localhost:5672', 
            'worker.queue': 'letv.spider.pipeline', 
            'mongodb': 'mongodb://localhost:27017', 
            'log.config': os.path.join(os.path.dirname(__file__), 'config', 'log4j.properties')
            }

    #opts, args = getopt.getopt(sys.argv[1:], 's:c:h', ['server=', 'log.config=', 'help'])
    
    opts, args = getopt.getopt(sys.argv[1:], 's:d:c:h', ['server=', 'mongodb=', 'log.config=', 'help'])
    for o, a in opts:
        if o == '-s' or o == '--server':
            options['server'] = a
        if o == '-d' or o == '--mongodb':
            options['mongodb'] = a
        elif o == '-c' or o == '--log.config':
            options['log.config'] = a
        elif o == '-h' or o == '--help':
            print '%s -s <server> -c <log.config>' % (sys.argv[0])
            sys.exit(0)

    # config logging
    logging.config.fileConfig(options['log.config'])

    consume_run(options)
