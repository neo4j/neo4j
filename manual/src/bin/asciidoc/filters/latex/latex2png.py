#!/usr/bin/env python
'''
NAME
    latex2png - Converts LaTeX source to PNG file

SYNOPSIS
    latex2png [options] INFILE

DESCRIPTION
    This filter reads LaTeX source text from the input file
    INFILE (or stdin if INFILE is -) and renders it to PNG image file.
    Typically used to render math equations.

    Requires latex(1), dvipng(1) commands and LaTeX math packages.

OPTIONS
    -D DPI
        Set the output resolution to DPI dots per inch. Use this option to
        scale the output image size.

    -o OUTFILE
        The file name of the output file. If not specified the output file is
        named like INFILE but with a .png file name extension.

    -m
        Skip if the PNG output file is newer that than the INFILE.
        Compares timestamps on INFILE and OUTFILE. If
        INFILE is - (stdin) then compares MD5 checksum stored in file
        named like OUTFILE but with a .md5 file name extension.
        The .md5 file is created if the -m option is used and the
        INFILE is - (stdin).

    -v
        Verbosely print processing information to stderr.

    --help, -h
        Print this documentation.

    --version
        Print program version number.

SEE ALSO
    latex(1), dvipng(1)

AUTHOR
    Written by Stuart Rackham, <srackham@gmail.com>
    The code was inspired by Kjell Magne Fauske's code:
    http://fauskes.net/nb/htmleqII/

    See also:
    http://www.amk.ca/python/code/mt-math
    http://code.google.com/p/latexmath2png/

COPYING
    Copyright (C) 2010 Stuart Rackham. Free use of this software is
    granted under the terms of the MIT License.
'''

# Suppress warning: "the md5 module is deprecated; use hashlib instead"
import warnings
warnings.simplefilter('ignore',DeprecationWarning)

import os, sys, tempfile, md5

VERSION = '0.1.0'

# Include LaTeX packages and commands here.
TEX_HEADER = r'''\documentclass{article}
\usepackage{amsmath}
\usepackage{amsthm}
\usepackage{amssymb}
\usepackage{bm}
\newcommand{\mx}[1]{\mathbf{\bm{#1}}} % Matrix command
\newcommand{\vc}[1]{\mathbf{\bm{#1}}} % Vector command
\newcommand{\T}{\text{T}}             % Transpose
\pagestyle{empty}
\begin{document}'''

TEX_FOOTER = r'''\end{document}'''

# Globals.
verbose = False

class EApp(Exception): pass     # Application specific exception.

def print_stderr(line):
    sys.stderr.write(line + os.linesep)

def print_verbose(line):
    if verbose:
        print_stderr(line)

def write_file(filename, data, mode='w'):
    f = open(filename, mode)
    try:
        f.write(data)
    finally:
        f.close()

def read_file(filename, mode='r'):
    f = open(filename, mode)
    try:
        return f.read()
    finally:
        f.close()

def run(cmd):
    global verbose
    if verbose:
        cmd += ' 1>&2'
    else:
        cmd += ' 2>%s 1>&2' % os.devnull
    print_verbose('executing: %s' % cmd)
    if os.system(cmd):
        raise EApp, 'failed command: %s' % cmd

def latex2png(infile, outfile, dpi, modified):
    '''Convert LaTeX input file infile to PNG file named outfile.'''
    outfile = os.path.abspath(outfile)
    outdir = os.path.dirname(outfile)
    if not os.path.isdir(outdir):
        raise EApp, 'directory does not exist: %s' % outdir
    texfile = tempfile.mktemp(suffix='.tex', dir=os.path.dirname(outfile))
    basefile = os.path.splitext(texfile)[0]
    dvifile = basefile + '.dvi'
    temps = [basefile + ext for ext in ('.tex','.dvi', '.aux', '.log')]
    skip = False
    if infile == '-':
        tex = sys.stdin.read()
        if modified:
            checksum = md5.new(tex).digest()
            md5_file = os.path.splitext(outfile)[0] + '.md5'
            if os.path.isfile(md5_file) and os.path.isfile(outfile) and \
                    checksum == read_file(md5_file,'rb'):
                skip = True
    else:
        if not os.path.isfile(infile):
            raise EApp, 'input file does not exist: %s' % infile
        tex = read_file(infile)
        if modified and os.path.isfile(outfile) and \
                os.path.getmtime(infile) <= os.path.getmtime(outfile):
            skip = True
    if skip:
        print_verbose('skipped: no change: %s' % outfile)
        return
    tex = '%s\n%s\n%s\n' % (TEX_HEADER, tex.strip(), TEX_FOOTER)
    print_verbose('tex:\n%s' % tex)
    write_file(texfile, tex)
    saved_pwd = os.getcwd()
    os.chdir(outdir)
    try:
        # Compile LaTeX document to DVI file.
        run('latex %s' % texfile)
        # Convert DVI file to PNG.
        cmd = 'dvipng'
        if dpi:
            cmd += ' -D %s' % dpi
        cmd += ' -T tight -x 1000 -z 9 -bg Transparent --truecolor -o "%s" "%s" ' \
               % (outfile,dvifile)
        run(cmd)
    finally:
        os.chdir(saved_pwd)
        for f in temps:
            if os.path.isfile(f):
                print_verbose('deleting: %s' % f)
                os.remove(f)
    if 'md5_file' in locals():
        print_verbose('writing: %s' % md5_file)
        write_file(md5_file, checksum, 'wb')

def usage(msg=''):
    if msg:
        print_stderr(msg)
    print_stderr('\n'
                 'usage:\n'
                 '    latex2png [options] INFILE\n'
                 '\n'
                 'options:\n'
                 '    -D DPI\n'
                 '    -o OUTFILE\n'
                 '    -m\n'
                 '    -v\n'
                 '    --help\n'
                 '    --version')

def main():
    # Process command line options.
    global verbose
    dpi = None
    outfile = None
    modified = False
    import getopt
    opts,args = getopt.getopt(sys.argv[1:], 'D:o:mhv', ['help','version'])
    for o,v in opts:
        if o in ('--help','-h'):
            print __doc__
            sys.exit(0)
        if o =='--version':
            print('latex2png version %s' % (VERSION,))
            sys.exit(0)
        if o == '-D': dpi = v
        if o == '-o': outfile = v
        if o == '-m': modified = True
        if o == '-v': verbose = True
    if len(args) != 1:
        usage()
        sys.exit(1)
    infile = args[0]
    if dpi and not dpi.isdigit():
        usage('invalid DPI')
        sys.exit(1)
    if outfile is None:
        if infile == '-':
            usage('OUTFILE must be specified')
            sys.exit(1)
        outfile = os.path.splitext(infile)[0] + '.png'
    # Do the work.
    latex2png(infile, outfile, dpi, modified)
    # Print something to suppress asciidoc 'no output from filter' warnings.
    if infile == '-':
        sys.stdout.write(' ')

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception, e:
        print_stderr("%s: %s" % (os.path.basename(sys.argv[0]), str(e)))
        sys.exit(1)
