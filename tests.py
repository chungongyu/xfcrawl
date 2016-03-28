# -*- coding: utf-8 -*-

import base64
import sys
from itertools import ifilter,imap
import string
import json

import pymongo
from redis import Redis

import dispatcher
import downloader
import extractor
import pipeline

def lines(f):
    for line in ifilter(lambda x: len(x)>0, imap(string.strip, f)):
        yield line

def dispatcher_test(tool, optionsi):
    if options.has_key('help'):
        return dispatcher_help(tool)

    options['redis'] = Redis.from_url(options['redis'])

    for line in lines(sys.stdin):
        parms = json.loads(line)
        links = dispatcher.consume_filter(options, **parms)

        data = {'wget': parms.get('wget', {})}
        for url in links:
            data['wget']['url'] = url
            print json.dumps(data)

    return 0

def dispatcher_help(tool):
    print '%s dispatch -h' % (tool)
    return 0

def downloader_test(tool, optionsi):
    if options.has_key('help'):
        return downloader_help(tool)

    for line in lines(sys.stdin):
        parms = json.loads(line)
        r = downloader.consume_request(options, **parms)
        parms['data'] = {'page': base64.b64encode(r.content), 'status': r.status_code}
        print json.dumps(parms)

    return 0

def downloader_help(tool):
    print '%s download -h' % (tool)
    return 0

def extractor_test(tool, options):
    if options.has_key('help'):
        return extractor_help(tool)

    mongodb = pymongo.MongoClient(options['mongodb'])

    for line in lines(sys.stdin):
        parms = json.loads(line)
        url_list, result = extractor.consume_extract(mongodb, **parms)
        parms['result'] = extractor.consume_patch(parms.get('result', {}), result)
        if url_list:
            data = {'wget': parms['wget'], 'data': {'links': url_list}}
            print 'downloader\t%s' % (json.dumps(data))
            """
            for url in url_list:
                parms['wget']['url'] = url
                print 'downloader\t%s' % (json.dumps(parms))
            """
        print 'pipeline\t%s' % (json.dumps(parms))

    return 0

def extractor_help(tool):
    print '%s extract -d <database> -h' % (tool)
    return 0

def pipeline_test(tool, options):
    if options.has_key('help'):
        return pipeline_help(tool)

    mongodb = pymongo.MongoClient(options['mongodb'])
    options['mongodb'] = mongodb

    for line in lines(sys.stdin):
        parms = json.loads(line)
        pipeline.consume_callback(None, None, None, line, options)

    return 0

def pipeline_help(tool):
    print '%s pipeline -d <database> -h' % (tool)
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
    import os
    import getopt
    import logging.config

    runner_list = {
            'dispatch': ['r:h', ['redis=', 'help'],      {'r': 'redis', 'h': 'help'},   dispatcher_test, 'dispatch urls'], 
            'download': ['h',   ['help'],                {'h': 'help'},                 downloader_test, 'download a url'], 
            'extract':  ['d:h', ['mongodb=', 'help'],    {'d': 'mongodb', 'h': 'help'}, extractor_test,  'extract meta data'], 
            'pipeline': ['d:h', ['mongodb=', 'help'],    {'d': 'mongodb', 'h': 'help'}, pipeline_test,   'save meta data'], 
            }

    if len(sys.argv) < 2 or not runner_list.has_key(sys.argv[1]):
        tools_help(sys.argv[0], runner_list)
        sys.exit(1)

    options, runner = {'log.config': os.path.join(os.path.dirname(__file__), 'config', 'log4j.properties')}, runner_list[sys.argv[1]]
    opts, args = getopt.getopt(sys.argv[2:], runner[0], runner[1])
    for o, a in opts:
        o = o.lstrip('-')
        if o in runner[2]:
            o = runner[2][o]
        options[o] = a
    
    # config logging
    logging.config.fileConfig(options['log.config'])

    sys.exit(runner[3](sys.argv[0], options))
