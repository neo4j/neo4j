#!/usr/bin/env python
'''
NAME
    code-filter - AsciiDoc filter to highlight language keywords

SYNOPSIS
    code-filter -b backend -l language [ -t tabsize ]
                [ --help | -h ] [ --version | -v ]

DESCRIPTION
    This filter reads source code from the standard input, highlights language
    keywords and comments and writes to the standard output.

    The purpose of this program is to demonstrate how to write an AsciiDoc
    filter -- it's much to simplistic to be passed off as a code syntax
    highlighter. Use the 'source-highlight-filter' instead.


OPTIONS
    --help, -h
        Print this documentation.

    -b
        Backend output file format: 'docbook', 'linuxdoc', 'html', 'css'.

    -l
        The name of the source code language: 'python', 'ruby', 'c++', 'c'.

    -t tabsize
        Expand source tabs to tabsize spaces.

    --version, -v
        Print program version number.

BUGS
    - Code on the same line as a block comment is treated as comment.
      Keywords inside literal strings are highlighted.
    - There doesn't appear to be an easy way to accomodate linuxdoc so
      just pass it through without markup.

AUTHOR
    Written by Stuart Rackham, <srackham@gmail.com>

URLS
    http://sourceforge.net/projects/asciidoc/
    http://www.methods.co.nz/asciidoc/

COPYING
    Copyright (C) 2002-2006 Stuart Rackham. Free use of this software is
    granted under the terms of the GNU General Public License (GPL).
'''

import os, sys, re, string

VERSION = '1.1.2'

# Globals.
language = None
backend = None
tabsize = 8
keywordtags = {
    'html':
        ('<strong>','</strong>'),
    'css':
        ('<strong>','</strong>'),
    'docbook':
        ('<emphasis role="strong">','</emphasis>'),
    'linuxdoc':
        ('','')
}
commenttags = {
    'html':
        ('<i>','</i>'),
    'css':
        ('<i>','</i>'),
    'docbook':
        ('<emphasis>','</emphasis>'),
    'linuxdoc':
        ('','')
}
keywords = {
    'python':
         ('and', 'del', 'for', 'is', 'raise', 'assert', 'elif', 'from',
         'lambda', 'return', 'break', 'else', 'global', 'not', 'try', 'class',
         'except', 'if', 'or', 'while', 'continue', 'exec', 'import', 'pass',
         'yield', 'def', 'finally', 'in', 'print'),
    'ruby':
        ('__FILE__', 'and', 'def', 'end', 'in', 'or', 'self', 'unless',
        '__LINE__', 'begin', 'defined?' 'ensure', 'module', 'redo', 'super',
        'until', 'BEGIN', 'break', 'do', 'false', 'next', 'rescue', 'then',
        'when', 'END', 'case', 'else', 'for', 'nil', 'retry', 'true', 'while',
        'alias', 'class', 'elsif', 'if', 'not', 'return', 'undef', 'yield'),
    'c++':
        ('asm', 'auto', 'bool', 'break', 'case', 'catch', 'char', 'class',
        'const', 'const_cast', 'continue', 'default', 'delete', 'do', 'double',
        'dynamic_cast', 'else', 'enum', 'explicit', 'export', 'extern',
        'false', 'float', 'for', 'friend', 'goto', 'if', 'inline', 'int',
        'long', 'mutable', 'namespace', 'new', 'operator', 'private',
        'protected', 'public', 'register', 'reinterpret_cast', 'return',
        'short', 'signed', 'sizeof', 'static', 'static_cast', 'struct',
        'switch', 'template', 'this', 'throw', 'true', 'try', 'typedef',
        'typeid', 'typename', 'union', 'unsigned', 'using', 'virtual', 'void',
        'volatile', 'wchar_t', 'while')
}
block_comments = {
    'python': ("'''","'''"),
    'ruby': None,
    'c++': ('/*','*/')
}
inline_comments = {
    'python': '#',
    'ruby': '#',
    'c++': '//'
}

def print_stderr(line):
    sys.stderr.write(line+os.linesep)

def sub_keyword(mo):
    '''re.subs() argument to tag keywords.'''
    word = mo.group('word')
    if word in keywords[language]:
        stag,etag = keywordtags[backend]
        return stag+word+etag
    else:
        return word

def code_filter():
    '''This function does all the work.'''
    global language, backend
    inline_comment = inline_comments[language]
    blk_comment = block_comments[language]
    if blk_comment:
        blk_comment = (re.escape(block_comments[language][0]),
            re.escape(block_comments[language][1]))
    stag,etag = commenttags[backend]
    in_comment = 0  # True if we're inside a multi-line block comment.
    tag_comment = 0 # True if we should tag the current line as a comment.
    line = sys.stdin.readline()
    while line:
        line = string.rstrip(line)
        line = string.expandtabs(line,tabsize)
        # Escape special characters.
        line = string.replace(line,'&','&amp;')
        line = string.replace(line,'<','&lt;')
        line = string.replace(line,'>','&gt;')
        # Process block comment.
        if blk_comment:
            if in_comment:
                if re.match(r'.*'+blk_comment[1]+r'$',line):
                    in_comment = 0
            else:
                if re.match(r'^\s*'+blk_comment[0]+r'.*'+blk_comment[1],line):
                    # Single line block comment.
                    tag_comment = 1
                elif re.match(r'^\s*'+blk_comment[0],line):
                    # Start of multi-line block comment.
                    tag_comment = 1
                    in_comment = 1
                else:
                    tag_comment = 0
        if tag_comment:
            if line: line = stag+line+etag
        else:
            if inline_comment:
                pos = string.find(line,inline_comment)
            else:
                pos = -1
            if pos >= 0:
                # Process inline comment.
                line = re.sub(r'\b(?P<word>\w+)\b',sub_keyword,line[:pos]) \
                    + stag + line[pos:] + etag
            else:
                line = re.sub(r'\b(?P<word>\w+)\b',sub_keyword,line)
        sys.stdout.write(line + os.linesep)
        line = sys.stdin.readline()

def usage(msg=''):
    if msg:
        print_stderr(msg)
    print_stderr('Usage: code-filter -b backend -l language [ -t tabsize ]')
    print_stderr('                   [ --help | -h ] [ --version | -v ]')

def main():
    global language, backend, tabsize
    # Process command line options.
    import getopt
    opts,args = getopt.getopt(sys.argv[1:],
        'b:l:ht:v',
        ['help','version'])
    if len(args) > 0:
        usage()
        sys.exit(1)
    for o,v in opts:
        if o in ('--help','-h'):
            print __doc__
            sys.exit(0)
        if o in ('--version','-v'):
            print('code-filter version %s' % (VERSION,))
            sys.exit(0)
        if o == '-b': backend = v
        if o == '-l':
            v = string.lower(v)
            if v == 'c': v = 'c++'
            language = v
        if o == '-t':
            try:
                tabsize = int(v)
            except:
                usage('illegal tabsize')
                sys.exit(1)
            if tabsize <= 0:
                usage('illegal tabsize')
                sys.exit(1)
    if backend is None:
        usage('backend option is mandatory')
        sys.exit(1)
    if not keywordtags.has_key(backend):
        usage('illegal backend option')
        sys.exit(1)
    if language is None:
        usage('language option is mandatory')
        sys.exit(1)
    if not keywords.has_key(language):
        usage('illegal language option')
        sys.exit(1)
    # Do the work.
    code_filter()

if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        print_stderr("%s: unexpected exit status: %s" %
            (os.path.basename(sys.argv[0]), sys.exc_info()[1]))
    # Exit with previous sys.exit() status or zero if no sys.exit().
    sys.exit(sys.exc_info()[1])
