# -*- coding: utf-8 -*-

from datetime import datetime
import json
import logging

import crontab
import pika
import pymongo

_logger = logging.getLogger('scheduler')

def scheduler_isvalid(feed):
    for key in ['id', 'cron', 'cmd_list']:
        if not feed.has_key(key):
            return False
    return True

def scheduler_now():
    fmt = '%Y-%m-%d %H:%M:00'
    return datetime.strptime(
            datetime.strftime(
                datetime.now(), fmt
            ), 
            fmt
        )

def scheduler_run(options):
    #########################################
    # now
    #########################################
    now = scheduler_now()

    _logger.info(now)

    #########################################
    # crontab
    #########################################
    
    # set up pika
    connection = pika.BlockingConnection(pika.URLParameters(options['server']))
    channel = connection.channel()
    for queue in options['client.queue']:
        channel.queue_declare(queue=queue)
    
    mongodb = pymongo.MongoClient(options['mongodb'])
    for feed in mongodb.spider.crontab.find({'flag':1}):
        if feed.has_key('_id'):
            del feed['_id']
        if scheduler_isvalid(feed):
            cron = crontab.CronTab(feed['cron'])
            if cron.test(now):
                _logger.info(json.dumps(feed))
                for cmd in feed['cmd_list']:
                    cmd['id'] = feed['id']
                    cmd['cron'] = feed['cron']
                    ##############################
                    #send it to scheduler
                    ##############################
                    channel.basic_publish(
                            exchange='', 
                            routing_key=options['client.queue'][0], 
                            body=json.dumps(cmd)
                            )
        else:
            _logger.error(json.dumps(feed))

if __name__ == '__main__':
    import os,sys
    import getopt
    import logging.config

    options = {
            'server': 'amqp://localhost:5672', 
            'client.queue': ('letv.spider.downloader',), 
            'mongodb': 'mongodb://localhost:27017', 
            'log.config': os.path.join(os.path.dirname(__file__), 'config', 'log4j.properties')
            }

    opts, args = getopt.getopt(sys.argv[1:], 's:d:c:h', ['server=', 'mongodb=', 'log.config=', 'help'])
    for o, a in opts:
        if o == '-s' or o == '--server':
            options['server'] = a
        if o == '-d' or o == '--mongodb':
            options['mongodb'] = a
        elif o == '-c' or o == '--log.config':
            options['log.config'] = a
        elif o == '-h' or o == '--help':
            print '%s -s <server> -d <mongodb> -c <log.config>' % (sys.argv[0])
            sys.exit(0)

    # config logging
    logging.config.fileConfig(options['log.config'])

    scheduler_run(options)
