# -*- coding: utf-8 -*-

import json
import logging
import re

_logger = logging.getLogger('rules')

class RuleItem:
    def __init__(self, text, attrs, actions=[]):
        self._text = text
        self._attrs = attrs
        self._actions = actions

    def process(self, datum):
        for action in self._actions:
            datum = action.process(datum)
        return datum

    def tojson(self):
        return {'text': self._text, 'attrs': self._attrs, 'actions': [r.tojson() for r in self._actions]}

    def __str__(self):
        return json.dumps(self.tojson())

    def attr(self, key, default=None):
        return self._attrs.get(key, default)

class XpathRule(RuleItem):
    def process(self, tree):
        datum = [line for line in tree.xpath(self._text)]
        return RuleItem.process(self, datum)

class RegRule(RuleItem):
    def process(self, datum):
        r = []

        connector = self._attrs.get('connector', '')
        pattern = re.compile(self._text, re.U|re.S)
        for data in datum:
            m = pattern.match(data)
            if m:
                r.append(connector.join([group for group in m.groups() if group]))

        return RuleItem.process(self, r)

class FilterRule(RuleItem):
    def process(self, datum):
        pattern = re.compile(self._text, re.U|re.S)
        datum = filter(lambda data: pattern.match(data), datum)
        return RuleItem.process(self, datum)


class RuleList:
    def __init__(self, rules):
        mapping = {
                'xpath': XpathRule, 
                'regex': RegRule, 
                'filter': FilterRule
                }

        self._rules = []
        for rule in rules:
            if rule.has_key('type') and mapping.has_key(rule['type']):
                self._rules.append(
                            mapping[rule['type']](
                                rule.get('text', ''), 
                                rule.get('attrs', {}), 
                                RuleList(rule.get('actions', {}))
                            ) 
                        )
    def process(self, doc):
        datum, urls = {}, []
        for rule in self._rules:
            data = rule.process(doc)
            key = rule.attr('key')
            if key:
                datum[key] = data
            if rule.attr('is_url', False):
                urls += data
        return urls, datum

    def __iter__(self):
        for rule in self._rules:
            yield rule

class PatternList:
    def __init__(self, patterns):
        self._patterns = []

        for pattern in patterns:
            self._patterns.append(
                (
                    pattern['pattern'], 
                    RuleList(pattern['rule_list'])
                )
            )

    def process(self, url, doc):
        for pattern, rule_list in self._patterns:
            if re.search(pattern, url):
                return rule_list.process(doc)
        return None, None

    def __iter__(self):
        for pattern, rule_list in self._patterns:
            yield pattern, rule_list

class Template:
    def __init__(self, template):
        self.id = template['id']
        self.typ = template['type']
        self._patterns = PatternList(template['pattern_list'])

    def process(self, url, doc):
        return self._patterns.process(url, doc)

if __name__ == '__main__':
    import os,sys
    import getopt

    import chardet
    from lxml import etree
    import pymongo

    options = {
            'mongodb': 'mongodb://localhost:27017', 
            }

    mongodb = pymongo.MongoClient(options['mongodb'])
    template = mongodb.spider.templates.find_one({'id':'movie.douban.com'})
    template = Template(template)
    
    data = sys.stdin.read()
    charset = chardet.detect(data)
    doc = etree.HTML(data, parser=etree.HTMLParser(encoding=charset['encoding']))

    urls, datum = template.process('http://movie.douban.com', doc)
    print urls, datum
