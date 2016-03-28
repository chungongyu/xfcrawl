# -*- coding: utf-8 -*-

import sys
from itertools import ifilter,imap
import string
import json

import pika
import pymongo

def rabbitmq_producer_run(tool, options):
    if options.has_key('help'):
        return rabbitmq_producer_help(tool)

    connection = pika.BlockingConnection(pika.URLParameters(options['server']))
    channel = connection.channel()
    
    for line in ifilter(lambda x: len(x)>0, imap(string.strip, sys.stdin)):
        try:
            channel.basic_publish(exchange='', 
                    routing_key=options['queue'], 
                    body=line
                    )
        except Exception, e:
            print e
    
    connection.close()

    return 0

def rabbitmq_producer_help(tool):
    print '%s rabbitmq-send -s <server> -q <queue> -h' % (tool)
    return 0

def rabbitmq_consumer_run(tool, optionsi):
    if options.has_key('help'):
        return rabbitmq_consumer_help(tool)

    connection = pika.BlockingConnection(pika.URLParameters(options['server']))
    channel = connection.channel()

    for i, (method, properties, body) in enumerate(channel.consume(queue=options['queue'])):
        print body
        channel.basic_ack(method.delivery_tag)
        if i + 1 >= int(options.get('count', 1)):
            break

    channel.cancel()
    channel.close()
    connection.close()

    return 0

def rabbitmq_consumer_help(tool):
    print '%s rabbitmq-receive -s <server> -q <queue> -h' % (tool)
    return 0

def mongodb_updater_run(tool, options):
    if options.has_key('help'):
        return mongodb_updater_help(tool)

    mongodb = pymongo.MongoClient(options['server'])
    for line in ifilter(lambda x: len(x)>0, imap(string.strip, sys.stdin)):
        data = json.loads(line)
        mongodb[options['database']][options['table']].update_one({'id':data['id']}, {'$set':data}, upsert=True)

    return 0

def mongodb_updater_help(tool):
    print '%s mongodb-update -s <server> -d <database> -t <table> -h' % (tool)
    return 0

def tools_help(tool, runner_list):
    print 'usage: %s <command> <options>' % (tool)
    print ''
    print 'The most commonly used commands are:'
    for cmd, (_, _, _, _, description) in runner_list.iteritems():
        print '%s\t%s' % (cmd, description)
    print ''
    print 'See %s <command> -h to read about a specific subcommand' % (tool)

if __name__ == '__main__':
    import getopt

    runner_list = {
            'rabbitmq-send':    ['s:q:h',   ['server=', 'queue=', 'help'],              {'s': 'server', 'q': 'queue', 'h': 'help'},                 rabbitmq_producer_run, 'publish a message'], 
            'rabbitmq-receive': ['s:q:n:h', ['server=', 'queue=', 'count=', 'help'],    {'s': 'server', 'q': 'queue', 'n': 'count', 'h': 'help'},   rabbitmq_consumer_run, 'consume a message'], 
            'mongodb-update':   ['s:d:t:h', ['server=', 'database=', 'table=', 'help'], {'s': 'server', 'd': 'database', 't': 'table', 'h': 'help'},mongodb_updater_run,   'update mongodb']
            }

    if len(sys.argv) < 2 or not runner_list.has_key(sys.argv[1]):
        tools_help(sys.argv[0], runner_list)
        sys.exit(1)

    options, runner = {}, runner_list[sys.argv[1]]
    opts, args = getopt.getopt(sys.argv[2:], runner[0], runner[1])
    for o, a in opts:
        o = o.lstrip('-')
        if o in runner[2]:
            o = runner[2][o]
        options[o] = a
    
    sys.exit(runner[3](sys.argv[0], options))
