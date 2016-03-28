# -*- coding: utf-8 -*-

def utf8(s):
    try: 
        return s.encode('utf8')
    except: 
        return s
