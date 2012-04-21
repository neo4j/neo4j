#!/usr/bin/env python
# -*- mode: Python; coding: utf-8 -*-

import sys

if not sys.argv[1].startswith('1'):
  try:
    sys.stdout.write(' ')
    sys.exit
  except IOError, e:
    if e.errno != 32:
      raise

title = ''
if len(sys.argv) > 2:
  title = sys.argv[2]
db = []
query = []
found_empty_line = False
data = sys.stdin.readlines()
for line in data:
  if found_empty_line:
    query.append(line)
  else:
    if len(line.strip()) == 0:
      found_empty_line = True
    else:
      db.append(line)
if len(db) == 0:
  sys.exit("A database has to be defined.")
if len(query) == 0:
  sys.exit("A query has to be defined.")
body = []
if len(title) > 0:
  body.append('<formalpara role="cypherconsole"><title>')
  body.append(title)
  body.append('</title><para>')
else:
  body.append('<simpara role="cypherconsole">')
body.append('<database>')
body.extend(db)
body.append('</database>')
body.append('<command>')
body.append(' '.join(query))
body.append('</command>')
if len(title) > 1:
  body.append('</para></formalpara>')
else:
  body.append('</simpara>')
sys.stdout.write(''.join(body))
  
