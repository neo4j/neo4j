#!/usr/bin/python

from subprocess import check_output
import sys
import datetime


def e(cmd, **kwargs):
  # Exec a command, performing shell substitution and
  # return a list of lines
  return check_output([cmd.format(**kwargs)], shell=True).split('\n')

def read_lines(filename, start_line, end_line):
    with open(filename, 'r') as f:
        lines = list(f)
        return lines[start_line-1:end_line]

def is_commented_out(string):
    return string.strip().startswith("//") or string.strip().startswith("/*")

def item_exists_in_file(filename, string):
    try:
        with open(filename, 'r') as f:
            return f.read().find(string) > -1
    except IOError as e:
        return False # No file, or error, oh well.

def find_deprecations():
    # Get all deprecated annotations
    for dep_line in e('grep -nr --include=*.java @Deprecated *'):

        if len(dep_line.strip()) > 0:
            # Each line looks like:
            # ./neo4j-community/target/javadoc-sources/org/neo4j/kernel/configuration/Config.java:376:    @Deprecated

            # Split to get file name and line no
            parts = dep_line.split(':')
            if len(parts) > 1:
                if not is_commented_out(parts[2]):
                    yield parts[0], parts[1]
            else:
                print "Unable to comprehend grep output '%s'" % dep_line

def find_identifiable_deprecated_items():
    ''' Generates three-tuples of filename, linenumber, item
    where item is a line in the file that either contains an @Deprecated
    annotation like so:
    
    @Deprecated void myFunc(){
    
    Or that immediately follows a line that contains only @Deprecated, like so:
    
    @Deprecated
    void myFunc() { <-- We return this line
    
    This can be used to find deprecated items after line numbers have changed.
    '''
    for filename, line in find_deprecations():
        first,second = read_lines(filename, int(line), int(line)+1)
        
        # If the line that contains @Deprecated contains more than that,
        # eg looks something like:
        # @Deprecated void myMethod() {
        # Then use that to figure out if the deprecated item has been removed,
        # otherwise use the next line
        if first.strip() == "@Deprecated\n":
            yield filename, line, second
        else:
            yield filename, line, first

def get_time_added(filename, line):
    # Get the timestamp for when a given line in a given file was added
    for line in e('git blame -L {line},{line} --incremental {file}', line=line,file=filename):
        if line.startswith('committer-time'):
            return int(line.split(' ')[1])
    raise Exception('Unable to figure out when line {line} was added to "{filename}".'.format(line=line,filename=filename))

def get_time_removed(filename, line):
    # Get the timestamp for when a given line in a given file was removed
    for line in e('git blame -L {line},{line} --reverse --incremental {file}', line=line,file=filename):
        if line.startswith('committer-time'):
            return int(line.split(' ')[1])
    raise Exception('Unable to figure out when line {line} was removed from "{filename}".'.format(line=line,filename=filename))

def get_tag_timestamp(tag):
    return int(e('git log --pretty=format:"%ct" -1 {tag}', tag=tag)[0])

#
# Higher level functions
#

def list_deprecated_before(tag):
    # Primary method exposed here, use this to get a list of (filename,line)
    # of @Deprecation's that were added before the given tag
    tag_timestamp = get_tag_timestamp(tag)

    for filename, line in find_deprecations():
        if tag_timestamp >= get_time_added(filename,line):
            yield filename, line
            
def list_deprecated_after(tag):
    # Use this to get a list of (filename,line)
    # of @Deprecation's that were added after the given tag
    tag_timestamp = get_tag_timestamp(tag)
    
    for filename, line in find_deprecations():
        if tag_timestamp < get_time_added(filename,line):
            yield filename, line

def list_removed_deprecations_between(added_before_tag, removed_at_tag):
    ''' Find all deprecated items added before a given tag
    that have subsequently been removed in a later tag.
    ''' 
    # Check out the tag we are interested in
    e('git checkout ' + added_before_tag)
    
    items = list(find_identifiable_deprecated_items())
    
    e('git checkout ' + removed_at_tag)
    
    for filename, line, item in items:
        if not item_exists_in_file(filename, item):
            print filename, item
    

