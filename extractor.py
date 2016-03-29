# -*- coding: utf-8 -*-

import base64
import json
import logging
import re
from string import Template
import urllib
import urlparse

import chardet
from lxml import etree
import pika
import pymongo

from rules import Template
import utils

_logger = logging.getLogger('extractor')

def consume_isvalid(**parms):
    if not parms.has_key('id'): return False
    if not parms.has_key('wget') or not parms['wget'].has_key('url'): return False
    if not parms.has_key('data') or not parms['data'].has_key('page') or len(parms['data']['page']) == 0: return False
    return True

def consume_get_template(mongodb, template_id):
    template = mongodb.spider.templates.find_one({'id':template_id})
    if template and template['flag'] == 1: # enabled
        return Template(template)
    return None

def consume_get_template2(mongodb, template_id):
    return mongodb.spider.templates.find_one({'id':template_id})

def consume_get_rulelist(mongodb, template_id, url):
    template = consume_get_template(mongodb, template_id)
    if template and template['flag'] == 1: # enabled
        for pattern in template['pattern_list']:
            if re.search(pattern['pattern'], url):
                return template['type'], pattern['rule_list']
    return None, None

def consume_absolute_url(url, path):
    if path.startswith('http://'):
        return path
    return urlparse.urljoin(url, path)

def consume_urlencode(url, encoding):
    url = url.encode(encoding)
    parts = list(urlparse.urlparse(url))
    parts[4] = urllib.urlencode(urlparse.parse_qsl(parts[4]))
    url = urlparse.urlunparse(parts)
    return url.decode(encoding)

def consume_extract_html(template, url, data):
    url_list, result = [], {}

    charset = chardet.detect(data)
    doc = etree.HTML(data, parser=etree.HTMLParser(encoding=charset['encoding']))

    return template.process(url, doc)

def consume_extract_html2(rule_list, url, data):
    def _extract_html_(rule, doc, url_list, result):
        tree = doc.getroottree()
        if rule.has_key('xpath'):
            node_list = doc.xpath(rule['xpath'])
            if rule.has_key('children'):
                for node in node_list: 
                    item = {}

                    for child in rule['children']:
                        _extract_html_(child, node, url_list, item)

                    if item:
                        if rule.has_key('key'):
                            if not result.has_key(rule['key']):
                                result[rule['key']] = []
                            result[rule['key']].append(item)
                        else:
                            result.update(item)
            else:
                for node in node_list:
                    node = node.strip()
                    if rule.has_key('type') and  rule['type'] == 'url':
                        url_list.append(
                                utils.utf8(
                                    consume_absolute_url(
                                        url, 
                                        consume_urlencode(
                                            node, tree.docinfo.encoding
                                        )
                                    )
                                )
                            )
                    if rule.has_key('key'):
                        result[rule['key']] = utils.utf8(node)

    url_list, result = [], {}

    charset = chardet.detect(data)

    doc = etree.HTML(data, parser=etree.HTMLParser(encoding=charset['encoding']))
    for rule in rule_list:
        _extract_html_(rule, doc, url_list, result)

    return url_list, result

def consume_extract_json(template, url, data):
    pass

def consume_extract_xml(template, url, data):
    pass

def consume_extract_text(template, url, data):
    pass

def consume_extract(mongodb, **parms):
    # callbacks
    extractor_list = {
            'html' : consume_extract_html, 
            'json' : consume_extract_json, 
            'xml' : consume_extract_xml, 
            'text' : consume_extract_text, 
            }

    url = utils.utf8(parms['wget']['url'])
    #template_type, rule_list = consume_get_rulelist(mongodb, parms['id'], url)
    template = consume_get_template(mongodb, parms['id'])
    if template and extractor_list.has_key(template.typ):
        return extractor_list[template.typ](template, url, base64.b64decode(parms['data']['page']))
    #if rule_list and extractor_list.has_key(template_type):
    #    return extractor_list[template_type](rule_list, url, base64.b64decode(parms['data']['page']))

    return None, None

def consume_patch(result, patch):
    result.update(patch)
    return result

def consume_callback(channel, method, properties, body, options):
    _logger.info(body)

    try:
        parms  = json.loads(body)
        if consume_isvalid(**parms):
            url_list, result = consume_extract(options['mongodb'], **parms)
            parms['result'] = consume_patch(parms.get('result', {}), result)
            if url_list:
                ###########################
                # send it to dispatcher
                ###########################
                data = {'id': parms['id'], 'wget': parms['wget'], 'data': {'links': url_list}}
                channel.basic_publish(exchange='', 
                        routing_key=options['client.queue'][0], 
                        body=json.dumps(data)
                        )
            ###########################
            # send it to pipeline
            ###########################
            channel.basic_publish(exchange='', 
                    routing_key=options['client.queue'][1], 
                    body = json.dumps(parms)
                    )
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
            'worker.queue': 'letv.spider.extractor', 
            'client.queue': ('letv.spider.dispatcher', 'letv.spider.pipeline'), 
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

    consume_run(options)
