#!/usr/bin/env python
# -*- mode: Python; coding: utf-8 -*-

import sys
from xml.sax.saxutils import escape
from xml.sax.saxutils import unescape

def out_entries(entries,simpara):
  buff = []
  buff.append('<row>')
  for entry in entries:
    buff.append('<entry align="left" valign="top">')
    if simpara:
      buff.append('<simpara><literal>')
    buff.append(entry.strip()) 
    if simpara:
      buff.append('</literal></simpara>')
    buff.append('</entry>')
  buff.append('</row>')
  return buff

def split_line(line):
  line = unescape(line)
  if not hasattr(split_line, 'positions'):
    split_line.positions = []
    left = 2
    right = -1
    for part in line[1:-2].split('|'):
      right += len(part) + 1
      split_line.positions.append( {'left':left, 'right':right} )
      left = right + 3
  strings = []
  for position in split_line.positions:
    strings.append(escape(line[position['left']:position['right']]))
  return strings

def table_header(title, headings, info_lines):
  if (headings):
    column_count = len(headings)
  else:
    column_count = 1
  if headings and len(headings[0]) == 0:
    sys.exit("The first table heading is empty.")
  buff = []
  if title:
    buff.append('<table tabstyle="queryresult table" role="NotInToc" frame="all" rowsep="1" colsep="1">')
    buff.append('<title>' + title + '</title>')
  else:
    buff.append('<informaltable tabstyle="queryresult table" frame="all" rowsep="1" colsep="1">')
  buff.append('<tgroup cols="')
  buff.append(str(column_count))
  buff.append('">')
  for i in range(1, column_count + 1):
    buff.append('<colspec colname="col')
    buff.append(str(i))
    buff.append('"/>')
  if (headings):
    buff.append('<thead>')
    buff.extend(out_entries(headings, 0))
    buff.append('</thead>')
  buff.append('<tfoot>')
  for line in info_lines:
    buff.append('<row><entry align="left" valign="top" namest="col1" nameend="col')
    buff.append(str(column_count))
    buff.append('">')
    buff.append(line)
    buff.append('</entry></row>')
  buff.append('</tfoot>')
  return buff


data = sys.stdin.readlines()
data.pop(0)
line = data.pop(0)
if line.startswith('| No data returned.'):
  first_line = False
  column_count = 1
else:
  first_line = split_line(line)
  column_count = len(first_line)
data.pop(0)
query_title = False
if len(sys.argv) > 1:
  query_title = sys.argv[1]
body = []
info_lines = []
body.append('<tbody>')
if first_line == False or data[0][0] == '+':
  if data[0][0] == '+':
    data.pop(0)
  body.append('<row><entry role="emptyresult" align="left" valign="top" namest="col1" nameend="col')
  body.append(str(column_count))
  body.append('"><simpara><literal>')
  body.append('(empty result)')
  body.append('</literal></simpara></entry></row>')
for line in data:
  if line[0] == '|':
    body.extend(out_entries(split_line(line), 1))
  elif line[0] != '+':
    info_lines.append(line)
body.append('</tbody>')

sys.stdout.write(''.join(table_header(query_title, first_line, info_lines)))
sys.stdout.write(''.join(body))
sys.stdout.write('</tgroup>')
if query_title:
  sys.stdout.write('</table>')
else:
  sys.stdout.write('</informaltable>')

