#!/usr/bin/python

from subprocess import check_output
import sys


def e(cmd, **kwargs):
  # Exec a command, performing shell substitution and
  # return a list of lines
  return check_output([cmd.format(**kwargs)], shell=True).split('\n')

def find_deprecations():
    # Get all deprecated annotations
    for dep_line in e('grep -nr --include=*.java @Deprecated *'):

        if len(dep_line.strip()) > 0:
            # Each line looks like:
            # ./neo4j-community/target/javadoc-sources/org/neo4j/kernel/configuration/Config.java:376:    @Deprecated

            # Split to get file name and line no
            filename, lineno, gunk = dep_line.split(':')

            yield filename, lineno

def get_time_added(filename, line):
    # Get the timestamp for when a given line in a given file was added
    for line in e('git blame -L {line},{line} --incremental {file}', line=line,file=filename):
        if line.startswith('committer-time'):
            return int(line.split(' ')[1])
    raise Exception('Unable to figure out when line {line} was added to "{filename}".'.format(line=line,filename=filename))

def get_tag_timestamp(tag):
    return int(e('git log --pretty=format:"%ct" -1 {tag}', tag=tag)[0])

def list_deprecated_before(tag):
    # Primary method exposed here, use this to get a list of (filename,line)
    # of @Deprecation's that were added before the given tag
    tag_timestamp = get_tag_timestamp(tag)

    for filename, line in find_deprecations():
        if tag_timestamp >= get_time_added(filename,line):
            yield filename, line


