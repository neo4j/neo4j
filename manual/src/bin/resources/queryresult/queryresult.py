#!/usr/bin/env python
# -*- mode: Python; coding: utf-8 -*-

import sys

data = sys.stdin.readlines()
lastline = data[:-1]
data.pop(0)
firstline = data.pop(0)[1:-2].split('|')
column_count = len(firstline)
data.pop(0)

sys.stdout.write( '<informaltable role="queryresult"><tgroup cols="')
sys.stdout.write(str(column_count))
sys.stdout.write('">')
for i in range(1, column_count):
  print '<colspec colname="col',i ,'"/>'
print '<thead><row>'
for entry in firstline:
  print '<entry>', entry.strip(), '</entry>'
print '</row></thead><tbody>'

for line in data:
  if line[0] == '|':
    print '<row>'
    entries = line[1:-2].split('|')
    for entry in entries:
      print '<entry>', entry.strip(), '</entry>'
    print '</row>'
print '</tbody>'
print '<tfoot><row>'
print '<entry namest="col1" nameend="col',column_count, '">', lastline, '</entry>'
print '</row></tfoot>'
print '</tgroup></informaltable>'

