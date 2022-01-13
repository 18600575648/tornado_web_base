import logging
import os
import stat
import sys
from collections import defaultdict
from pathlib import Path, PurePath
from uuid import uuid1

import six
import ujson as json
from genericpath import exists


def to_dict(MyClass, obj):
  # dict
  if MyClass == dict:        
    return {k: (v if type(v) in (int, str, bool, type(None)) else to_dict(type(v), v))
    for k, v in obj.items() if not callable(v)}
  # list
  if MyClass == list:
    return [v if type(v) in (int, str, bool, type(None)) else to_dict(type(v), v) for v in obj if not callable(v)]
  # other object
  class_vars = vars(MyClass)  # get any "default" attrs defined at the class level
  inst_vars = vars(obj)  # get any attrs defined on the instance (self)
  all_vars = dict(class_vars)
  all_vars.update(inst_vars)
  # filter out private attributes
  public_vars = {k: (v if type(v) in (int, str, bool, type(None)) else to_dict(type(v), v))
  for k, v in all_vars.items() if not k.startswith('_') and not callable(v)}
  return public_vars

def createDirIfNotExists(dir):  
  if not os.path.exists(dir):  
    os.makedirs(dir, mode=0o777, exist_ok=True)  
  
def createIfNotExists(file):
  location = PurePath(file).parent

  os.makedirs(location, exist_ok=True)

  if not exists(file):
      with open(file, "w") as f:
          logging.getLogger().info(f"create file {file}")

def saveToFile(file, content):
    """
    保存文件
    @param file: 文件名
    @param content: 内容
    """
    with open(file, "w") as f:        
        f.write(json.dumps(content, indent=True))
        f.flush()    

def merge(d1, d2):
    d1 = {} if not d1 else d1
    d2 = {} if not d2 else d2
    d1.update(d2)
    return defaultdict(str,d1)

def consoleSingleLine(message):    
    if not 'incloud' in os.environ:
      message = f'\r{message}  '
      print(message, end='')
      sys.stdout.flush()
    else:
      logging.getLogger().info(message)
      
def abstract(dict1, keys):
  if not dict1 or not keys:
    return dict1
  return dict1 if not dict1 or not keys else {key:dict1.get(key, None) for key in keys}

def abstractValue(dict1, keys, default=''):
  if not dict1 or not keys:
    return dict1
  if type(keys) == str:
    keys=keys.split(',')
  return list(({key:dict1.get(key.strip(), default) for key in keys}).values())


def guid():
  return str(uuid1()).replace("-", "")

unichr = chr
_escape_table = [unichr(x) for x in range(128)]
_escape_table[0] = u'\\0'
_escape_table[ord('\\')] = u'\\\\'
_escape_table[ord('\n')] = u'\\n'
_escape_table[ord('\r')] = u'\\r'
_escape_table[ord('\032')] = u'\\Z'
_escape_table[ord('"')] = u'\\"'
#_escape_table[ord("'")] = u"\\'"   #为兼容JSONData数据字符串自动给出两个单引号

def _escape_unicode(value, mapping=None):
    """escapes *value* without adding quote.
    Value should be unicode
    """
    return value.translate(_escape_table)

escape_string = _escape_unicode
