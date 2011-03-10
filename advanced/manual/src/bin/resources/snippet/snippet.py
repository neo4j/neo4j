#!/usr/bin/env python
# -*- mode: Python; coding: utf-8 -*-

import sys

def configuration(indata):
    config = {}
    for line in indata:
        line = line.strip()
        if not line: continue
        try:
            key, value = line.split('=',1)
        except:
            raise ValueError('invalid config line: "%s"' % (line,))
        config[key] = value
    return config

def snippet(source=None, component=None, classifier="test-sources", tag=None,
            **other):
    for key in other:
        sys.stderr.write("WARNING: unknown config key: '%s'\n" % key)
    if not tag: raise ValueError("'tag' must be specified")
    if not source: raise ValueError("'source' must be specified")
    if not component: raise ValueError("'component' must be specified")
    if not classifier: raise ValueError("'classifier' must be specified")

    START = "START SNIPPET: %s" % tag
    END = "END SNIPPET: %s" % tag

    sourceFile = open("target/%(classifier)s/%(component)s-%(classifier)s-jar"
                      "/%(source)s" % locals())
    try:
        emit = False
        for line in sourceFile:
            if END in line: emit = False
            if emit: sys.stdout.write(line.replace(']]>',']]>]]&gt;<![CDATA['))
            if START in line: emit = True
    finally:
        sourceFile.close()

if __name__ == '__main__':
    import traceback
    try:
        sys.stdout.write("<![CDATA[")
        snippet(**configuration(sys.stdin))
    except:
        traceback.print_exc(file=sys.stdout)
        raise
    finally:
        sys.stdout.write("]]>")
