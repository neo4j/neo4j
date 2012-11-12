#!/usr/bin/env python
'''
a2x - A toolchain manager for AsciiDoc (converts Asciidoc text files to other
      file formats)

Copyright: Stuart Rackham (c) 2009
License:   MIT
Email:     srackham@gmail.com

'''

import os
import fnmatch
import HTMLParser
import re
import shutil
import subprocess
import sys
import traceback
import urlparse
import zipfile
import xml.dom.minidom
import mimetypes

PROG = os.path.basename(os.path.splitext(__file__)[0])
VERSION = '8.6.7'

# AsciiDoc global configuration file directory.
# NOTE: CONF_DIR is "fixed up" by Makefile -- don't rename or change syntax.
CONF_DIR = '/etc/asciidoc'


######################################################################
# Default configuration file parameters.
######################################################################

# Optional environment variable dictionary passed to
# executing programs. If set to None the existing
# environment is used.
ENV = None

# External executables.
ASCIIDOC = 'asciidoc'
XSLTPROC = 'xsltproc'
DBLATEX = 'dblatex'         # pdf generation.
FOP = 'fop'                 # pdf generation (--fop option).
W3M = 'w3m'                 # text generation.
LYNX = 'lynx'               # text generation (if no w3m).
XMLLINT = 'xmllint'         # Set to '' to disable.
EPUBCHECK = 'epubcheck'     # Set to '' to disable.
# External executable default options.
ASCIIDOC_OPTS = ''
DBLATEX_OPTS = ''
FOP_OPTS = ''
XSLTPROC_OPTS = ''
BACKEND_OPTS = ''

######################################################################
# End of configuration file parameters.
######################################################################


#####################################################################
# Utility functions
#####################################################################

OPTIONS = None  # These functions read verbose and dry_run command options.

def errmsg(msg):
    sys.stderr.write('%s: %s\n' % (PROG,msg))

def warning(msg):
    errmsg('WARNING: %s' % msg)

def infomsg(msg):
    print '%s: %s' % (PROG,msg)

def die(msg, exit_code=1):
    errmsg('ERROR: %s' % msg)
    sys.exit(exit_code)

def trace():
    """Print traceback to stderr."""
    errmsg('-'*60)
    traceback.print_exc(file=sys.stderr)
    errmsg('-'*60)

def verbose(msg):
    if OPTIONS.verbose or OPTIONS.dry_run:
        infomsg(msg)

class AttrDict(dict):
    """
    Like a dictionary except values can be accessed as attributes i.e. obj.foo
    can be used in addition to obj['foo'].
    If self._default has been set then it will be returned if a non-existant
    attribute is accessed (instead of raising an AttributeError).
    """
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError, k:
            if self.has_key('_default'):
                return self['_default']
            else:
                raise AttributeError, k
    def __setattr__(self, key, value):
        self[key] = value
    def __delattr__(self, key):
        try: del self[key]
        except KeyError, k: raise AttributeError, k
    def __repr__(self):
        return '<AttrDict ' + dict.__repr__(self) + '>'
    def __getstate__(self):
        return dict(self)
    def __setstate__(self,value):
        for k,v in value.items(): self[k]=v

def isexecutable(file_name):
    return os.path.isfile(file_name) and os.access(file_name, os.X_OK)

def find_executable(file_name):
    '''
    Search for executable file_name in the system PATH.
    Return full path name or None if not found.
    '''
    def _find_executable(file_name):
        if os.path.split(file_name)[0] != '':
            # file_name includes directory so don't search path.
            if not isexecutable(file_name):
                return None
            else:
                return file_name
        for p in os.environ.get('PATH', os.defpath).split(os.pathsep):
            f = os.path.join(p, file_name)
            if isexecutable(f):
                return os.path.realpath(f)
        return None
    if os.name == 'nt' and os.path.splitext(file_name)[1] == '':
        for ext in ('.cmd','.bat','.exe'):
            result = _find_executable(file_name + ext)
            if result: break
    else:
        result = _find_executable(file_name)
    return result

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

def shell_cd(path):
    verbose('chdir %s' % path)
    if not OPTIONS.dry_run:
        os.chdir(path)

def shell_makedirs(path):
    if os.path.isdir(path):
        return
    verbose('creating %s' % path)
    if not OPTIONS.dry_run:
        os.makedirs(path)

def shell_copy(src, dst):
    verbose('copying "%s" to "%s"' % (src,dst))
    if not OPTIONS.dry_run:
        shutil.copy(src, dst)

def shell_rm(path):
    if not os.path.exists(path):
        return
    verbose('deleting %s' % path)
    if not OPTIONS.dry_run:
        os.unlink(path)

def shell_rmtree(path):
    if not os.path.isdir(path):
        return
    verbose('deleting %s' % path)
    if not OPTIONS.dry_run:
        shutil.rmtree(path)

def shell(cmd, raise_error=True):
    '''
    Execute command cmd in shell and return tuple
    (stdoutdata, stderrdata, returncode).
    If raise_error is True then a non-zero return terminates the application.
    '''
    if os.name == 'nt':
        # TODO: this is probably unnecessary, see:
        # http://groups.google.com/group/asciidoc/browse_frm/thread/9442ee0c419f1242
        # Windows doesn't like running scripts directly so explicitly
        # specify python interpreter.
        # Extract first (quoted or unquoted) argument.
        mo = re.match(r'^\s*"\s*(?P<arg0>[^"]+)\s*"', cmd)
        if not mo:
            mo = re.match(r'^\s*(?P<arg0>[^ ]+)', cmd)
        if mo.group('arg0').endswith('.py'):
            cmd = 'python ' + cmd
        # Remove redundant quoting -- this is not just cosmetic,
        # quoting seems to dramatically decrease the allowed command
        # length in Windows XP.
        cmd = re.sub(r'"([^ ]+?)"', r'\1', cmd)
    verbose('executing: %s' % cmd)
    if OPTIONS.dry_run:
        return
    stdout = stderr = subprocess.PIPE
    try:
        popen = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
                shell=True, env=ENV)
    except OSError, e:
        die('failed: %s: %s' % (cmd, e))
    stdoutdata, stderrdata = popen.communicate()
    if OPTIONS.verbose:
        print stdoutdata
        print stderrdata
    if popen.returncode != 0 and raise_error:
        die('%s returned non-zero exit status %d' % (cmd, popen.returncode))
    return (stdoutdata, stderrdata, popen.returncode)

def find_resources(files, tagname, attrname, filter=None):
    '''
    Search all files and return a list of local URIs from attrname attribute
    values in tagname tags.
    Handles HTML open and XHTML closed tags.
    Non-local URIs are skipped.
    files can be a file name or a list of file names.
    The filter function takes a dictionary of tag attributes and returns True if
    the URI is to be included.
    '''
    class FindResources(HTMLParser.HTMLParser):
        # Nested parser class shares locals with enclosing function.
        def handle_startendtag(self, tag, attrs):
            self.handle_starttag(tag, attrs)
        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == tagname and (filter is None or filter(attrs)):
                # Accept only local URIs.
                uri = urlparse.urlparse(attrs[attrname])
                if uri[0] in ('','file') and not uri[1] and uri[2]:
                    result.append(uri[2])
    if isinstance(files, str):
        files = [files]
    result = []
    for filename in files:
        verbose('finding resources in: %s' % filename)
        if OPTIONS.dry_run:
            continue
        parser = FindResources()
        # HTMLParser has problems with non-ASCII strings.
        # See http://bugs.python.org/issue3932
        contents = read_file(filename)
        mo = re.search(r'\A<\?xml.* encoding="(.*?)"', contents)
        if mo:
            encoding = mo.group(1)
            parser.feed(contents.decode(encoding))
        else:
            parser.feed(contents)
        parser.close()
    result = list(set(result))   # Drop duplicate values.
    result.sort()
    return result

# NOT USED.
def copy_files(files, src_dir, dst_dir):
    '''
    Copy list of relative file names from src_dir to dst_dir.
    '''
    for filename in files:
        filename = os.path.normpath(filename)
        if os.path.isabs(filename):
            continue
        src = os.path.join(src_dir, filename)
        dst = os.path.join(dst_dir, filename)
        if not os.path.exists(dst):
            if not os.path.isfile(src):
                warning('missing file: %s' % src)
                continue
            dstdir = os.path.dirname(dst)
            shell_makedirs(dstdir)
            shell_copy(src, dst)

def find_files(path, pattern):
    '''
    Return list of file names matching pattern in directory path.
    '''
    result = []
    for (p,dirs,files) in os.walk(path):
        for f in files:
            if fnmatch.fnmatch(f, pattern):
                result.append(os.path.normpath(os.path.join(p,f)))
    return result

def exec_xsltproc(xsl_file, xml_file, dst_dir, opts = ''):
    cwd = os.getcwd()
    shell_cd(dst_dir)
    try:
        shell('"%s" %s "%s" "%s"' % (XSLTPROC, opts, xsl_file, xml_file))
    finally:
        shell_cd(cwd)

def get_source_options(asciidoc_file):
    '''
    Look for a2x command options in AsciiDoc source file.
    Limitation: options cannot contain double-quote characters.
    '''
    def parse_options():
        # Parse options to result sequence.
        inquotes = False
        opt = ''
        for c in options:
            if c == '"':
                if inquotes:
                    result.append(opt)
                    opt = ''
                    inquotes = False
                else:
                    inquotes = True
            elif c == ' ':
                if inquotes:
                    opt += c
                elif opt:
                    result.append(opt)
                    opt = ''
            else:
                opt += c
        if opt:
            result.append(opt)

    result = []
    if os.path.isfile(asciidoc_file):
        options = ''
        f = open(asciidoc_file)
        try:
            for line in f:
                mo = re.search(r'^//\s*a2x:', line)
                if mo:
                    options += ' ' + line[mo.end():].strip()
        finally:
            f.close()
        parse_options()
    return result


#####################################################################
# Application class
#####################################################################

class A2X(AttrDict):
    '''
    a2x options and conversion functions.
    '''

    def execute(self):
        '''
        Process a2x command.
        '''
        self.process_options()
        # Append configuration file options.
        self.asciidoc_opts += ' ' + ASCIIDOC_OPTS
        self.dblatex_opts  += ' ' + DBLATEX_OPTS
        self.fop_opts      += ' ' + FOP_OPTS
        self.xsltproc_opts += ' ' + XSLTPROC_OPTS
        self.backend_opts  += ' ' + BACKEND_OPTS
        # Execute to_* functions.
        if self.backend:
            self.to_backend()
        else:
            self.__getattribute__('to_'+self.format)()
        if not (self.keep_artifacts or self.format == 'docbook' or self.skip_asciidoc):
            shell_rm(self.dst_path('.xml'))

    def load_conf(self):
        '''
        Load a2x configuration file from default locations and --conf-file
        option.
        '''
        global ASCIIDOC
        CONF_FILE = 'a2x.conf'
        a2xdir = os.path.dirname(os.path.realpath(__file__))
        conf_files = []
        # From a2x.py directory.
        conf_files.append(os.path.join(a2xdir, CONF_FILE))
        # If the asciidoc executable and conf files are in the a2x directory
        # then use the local copy of asciidoc and skip the global a2x conf.
        asciidoc = os.path.join(a2xdir, 'asciidoc.py')
        asciidoc_conf = os.path.join(a2xdir, 'asciidoc.conf')
        if os.path.isfile(asciidoc) and os.path.isfile(asciidoc_conf):
            self.asciidoc = asciidoc
        else:
            self.asciidoc = None
            # From global conf directory.
            conf_files.append(os.path.join(CONF_DIR, CONF_FILE))
        # From $HOME directory.
        home_dir = os.environ.get('HOME')
        if home_dir is not None:
            conf_files.append(os.path.join(home_dir, '.asciidoc', CONF_FILE))
        # If asciidoc is not local to a2x then search the PATH.
        if not self.asciidoc:
            self.asciidoc = find_executable(ASCIIDOC)
            if not self.asciidoc:
                die('unable to find asciidoc: %s' % ASCIIDOC)
        # From backend plugin directory.
        if self.backend is not None:
            stdout = shell(self.asciidoc + ' --backend list')[0]
            backends = [(i, os.path.split(i)[1]) for i in stdout.splitlines()]
            backend_dir = [i[0] for i in backends if i[1] == self.backend]
            if len(backend_dir) == 0:
                die('missing %s backend' % self.backend)
            if len(backend_dir) > 1:
                die('more than one %s backend' % self.backend)
            verbose('found %s backend directory: %s' %
                    (self.backend, backend_dir[0]))
            conf_files.append(os.path.join(backend_dir[0], 'a2x-backend.py'))
        # From --conf-file option.
        if self.conf_file is not None:
            if not os.path.isfile(self.conf_file):
                die('missing configuration file: %s' % self.conf_file)
            conf_files.append(self.conf_file)
        # From --xsl-file option.
        if self.xsl_file is not None:
            if not os.path.isfile(self.xsl_file):
                die('missing XSL file: %s' % self.xsl_file)
            self.xsl_file = os.path.abspath(self.xsl_file)
        # Load ordered files.
        for f in conf_files:
            if os.path.isfile(f):
                verbose('loading configuration file: %s' % f)
                execfile(f, globals())

    def process_options(self):
        '''
        Validate and command options and set defaults.
        '''
        if not os.path.isfile(self.asciidoc_file):
            die('missing SOURCE_FILE: %s' % self.asciidoc_file)
        self.asciidoc_file = os.path.abspath(self.asciidoc_file)
        if not self.destination_dir:
            self.destination_dir = os.path.dirname(self.asciidoc_file)
        else:
            if not os.path.isdir(self.destination_dir):
                die('missing --destination-dir: %s' % self.destination_dir)
            self.destination_dir = os.path.abspath(self.destination_dir)
        self.resource_dirs = []
        self.resource_files = []
        if self.resource_manifest:
            if not os.path.isfile(self.resource_manifest):
                die('missing --resource-manifest: %s' % self.resource_manifest)
            f = open(self.resource_manifest)
            try:
                for r in f:
                    self.resources.append(r.strip())
            finally:
                f.close()
        for r in self.resources:
            r = os.path.expanduser(r)
            r = os.path.expandvars(r)
            if r.endswith('/') or r.endswith('\\'):
                if  os.path.isdir(r):
                    self.resource_dirs.append(r)
                else:
                    die('missing resource directory: %s' % r)
            elif os.path.isdir(r):
                self.resource_dirs.append(r)
            elif r.startswith('.') and '=' in r:
                ext, mimetype = r.split('=')
                mimetypes.add_type(mimetype, ext)
            else:
                self.resource_files.append(r)
        for p in (os.path.dirname(self.asciidoc), CONF_DIR):
            for d in ('images','stylesheets'):
                d = os.path.join(p,d)
                if os.path.isdir(d):
                    self.resource_dirs.append(d)
        verbose('resource files: %s' % self.resource_files)
        verbose('resource directories: %s' % self.resource_dirs)
        if not self.doctype and self.format == 'manpage':
            self.doctype = 'manpage'
        if self.doctype:
            self.asciidoc_opts += ' --doctype %s' % self.doctype
        for attr in self.attributes:
            self.asciidoc_opts += ' --attribute "%s"' % attr
#        self.xsltproc_opts += ' --nonet'
        if self.verbose:
            self.asciidoc_opts += ' --verbose'
            self.dblatex_opts += ' -V'
        if self.icons or self.icons_dir:
            params = [
                'callout.graphics 1',
                'navig.graphics 1',
                'admon.textlabel 0',
                'admon.graphics 1',
            ]
            if self.icons_dir:
                params += [
                    'admon.graphics.path "%s/"' % self.icons_dir,
                    'callout.graphics.path "%s/callouts/"' % self.icons_dir,
                    'navig.graphics.path "%s/"' % self.icons_dir,
                ]
        else:
            params = [
                'callout.graphics 0',
                'navig.graphics 0',
                'admon.textlabel 1',
                'admon.graphics 0',
            ]
        if self.stylesheet:
            params += ['html.stylesheet "%s"' % self.stylesheet]
        if self.format == 'htmlhelp':
            params += ['htmlhelp.chm "%s"' % self.basename('.chm'),
                       'htmlhelp.hhp "%s"' % self.basename('.hhp'),
                       'htmlhelp.hhk "%s"' % self.basename('.hhk'),
                       'htmlhelp.hhc "%s"' % self.basename('.hhc')]
        if self.doctype == 'book':
            params += ['toc.section.depth 1']
            # Books are chunked at chapter level.
            params += ['chunk.section.depth 0']
        for o in params:
            if o.split()[0]+' ' not in self.xsltproc_opts:
                self.xsltproc_opts += ' --stringparam ' + o
        if self.fop_opts:
            self.fop = True
        if os.path.splitext(self.asciidoc_file)[1].lower() == '.xml':
            self.skip_asciidoc = True
        else:
            self.skip_asciidoc = False

    def dst_path(self, ext):
        '''
        Return name of file or directory in the destination directory with
        the same name as the asciidoc source file but with extension ext.
        '''
        return os.path.join(self.destination_dir, self.basename(ext))

    def basename(self, ext):
        '''
        Return the base name of the asciidoc source file but with extension
        ext.
        '''
        return os.path.basename(os.path.splitext(self.asciidoc_file)[0]) + ext

    def asciidoc_conf_file(self, path):
        '''
        Return full path name of file in asciidoc configuration files directory.
        Search first the directory containing the asciidoc executable then
        the global configuration file directory.
        '''
        f = os.path.join(os.path.dirname(self.asciidoc), path)
        if not os.path.isfile(f):
            f = os.path.join(CONF_DIR, path)
            if not os.path.isfile(f):
                die('missing configuration file: %s' % f)
        return os.path.normpath(f)

    def xsl_stylesheet(self, file_name=None):
        '''
        Return full path name of file in asciidoc docbook-xsl configuration
        directory.
        If an XSL file was specified with the --xsl-file option then it is
        returned.
        '''
        if self.xsl_file is not None:
            return self.xsl_file
        if not file_name:
            file_name = self.format + '.xsl'
        return self.asciidoc_conf_file(os.path.join('docbook-xsl', file_name))

    def copy_resources(self, html_files, src_dir, dst_dir, resources=[]):
        '''
        Search html_files for images and CSS resource URIs (html_files can be a
        list of file names or a single file name).
        Copy them from the src_dir to the dst_dir.
        If not found in src_dir then recursively search all specified
        resource directories.
        Optional additional resources files can be passed in the resources list.
        '''
        resources = resources[:]
        resources += find_resources(html_files, 'link', 'href',
                        lambda attrs: attrs.get('type') == 'text/css')
        resources += find_resources(html_files, 'img', 'src')
        resources += self.resource_files
        resources = list(set(resources))    # Drop duplicates.
        resources.sort()
        for f in resources:
            if '=' in f:
                src, dst = f.split('=')
                if not dst:
                    dst = src
            else:
                src = dst = f
            src = os.path.normpath(src)
            dst = os.path.normpath(dst)
            if os.path.isabs(dst):
                die('absolute resource file name: %s' % dst)
            if dst.startswith(os.pardir):
                die('resource file outside destination directory: %s' % dst)
            src = os.path.join(src_dir, src)
            dst = os.path.join(dst_dir, dst)
            if not os.path.isfile(src):
                for d in self.resource_dirs:
                    d = os.path.join(src_dir, d)
                    found = find_files(d, os.path.basename(src))
                    if found:
                        src = found[0]
                        break
                else:
                    if not os.path.isfile(dst):
                        die('missing resource: %s' % src)
                    continue
            # Arrive here if resource file has been found.
            if os.path.normpath(src) != os.path.normpath(dst):
                dstdir = os.path.dirname(dst)
                shell_makedirs(dstdir)
                shell_copy(src, dst)

    def to_backend(self):
        '''
        Convert AsciiDoc source file to a backend output file using the global
        'to_<backend name>' function (loaded from backend plugin a2x-backend.py
        file).
        Executes the global function in an A2X class instance context.
        '''
        eval('to_%s(self)' % self.backend)

    def to_docbook(self):
        '''
        Use asciidoc to convert asciidoc_file to DocBook.
        args is a string containing additional asciidoc arguments.
        '''
        docbook_file = self.dst_path('.xml')
        if self.skip_asciidoc:
            if not os.path.isfile(docbook_file):
                die('missing docbook file: %s' % docbook_file)
            return
        shell('"%s" --backend docbook -a "a2x-format=%s" %s --out-file "%s" "%s"' %
             (self.asciidoc, self.format, self.asciidoc_opts, docbook_file, self.asciidoc_file))
        if not self.no_xmllint and XMLLINT:
            shell('"%s" --nonet --noout --valid "%s"' % (XMLLINT, docbook_file))

    def to_xhtml(self):
        self.to_docbook()
        docbook_file = self.dst_path('.xml')
        xhtml_file = self.dst_path('.html')
        opts = '%s --output "%s"' % (self.xsltproc_opts, xhtml_file)
        exec_xsltproc(self.xsl_stylesheet(), docbook_file, self.destination_dir, opts)
        src_dir = os.path.dirname(self.asciidoc_file)
        self.copy_resources(xhtml_file, src_dir, self.destination_dir)

    def to_manpage(self):
        self.to_docbook()
        docbook_file = self.dst_path('.xml')
        opts = self.xsltproc_opts
        exec_xsltproc(self.xsl_stylesheet(), docbook_file, self.destination_dir, opts)

    def to_pdf(self):
        if self.fop:
            self.exec_fop()
        else:
            self.exec_dblatex()

    def exec_fop(self):
        self.to_docbook()
        docbook_file = self.dst_path('.xml')
        xsl = self.xsl_stylesheet('fo.xsl')
        fo = self.dst_path('.fo')
        pdf = self.dst_path('.pdf')
        opts = '%s --output "%s"' % (self.xsltproc_opts, fo)
        exec_xsltproc(xsl, docbook_file, self.destination_dir, opts)
        shell('"%s" %s -fo "%s" -pdf "%s"' % (FOP, self.fop_opts, fo, pdf))
        if not self.keep_artifacts:
            shell_rm(fo)

    def exec_dblatex(self):
        self.to_docbook()
        docbook_file = self.dst_path('.xml')
        xsl = self.asciidoc_conf_file(os.path.join('dblatex','asciidoc-dblatex.xsl'))
        sty = self.asciidoc_conf_file(os.path.join('dblatex','asciidoc-dblatex.sty'))
        shell('"%s" -t %s -p "%s" -s "%s" %s "%s"' %
             (DBLATEX, self.format, xsl, sty, self.dblatex_opts, docbook_file))

    def to_dvi(self):
        self.exec_dblatex()

    def to_ps(self):
        self.exec_dblatex()

    def to_tex(self):
        self.exec_dblatex()

    def to_htmlhelp(self):
        self.to_chunked()

    def to_chunked(self):
        self.to_docbook()
        docbook_file = self.dst_path('.xml')
        opts = self.xsltproc_opts
        xsl_file = self.xsl_stylesheet()
        if self.format == 'chunked':
            dst_dir = self.dst_path('.chunked')
        elif self.format == 'htmlhelp':
            dst_dir = self.dst_path('.htmlhelp')
        if not 'base.dir ' in opts:
            opts += ' --stringparam base.dir "%s/"' % os.path.basename(dst_dir)
        # Create content.
        shell_rmtree(dst_dir)
        shell_makedirs(dst_dir)
        exec_xsltproc(xsl_file, docbook_file, self.destination_dir, opts)
        html_files = find_files(dst_dir, '*.html')
        src_dir = os.path.dirname(self.asciidoc_file)
        self.copy_resources(html_files, src_dir, dst_dir)

    def update_epub_manifest(self, opf_file):
        '''
        Scan the OEBPS directory for any files that have not been registered in
        the OPF manifest then add them to the manifest.
        '''
        opf_dir = os.path.dirname(opf_file)
        resource_files = []
        for (p,dirs,files) in os.walk(os.path.dirname(opf_file)):
            for f in files:
                f = os.path.join(p,f)
                if os.path.isfile(f):
                    assert f.startswith(opf_dir)
                    f = '.' + f[len(opf_dir):]
                    f = os.path.normpath(f)
                    if f not in ['content.opf']:
                        resource_files.append(f)
        opf = xml.dom.minidom.parseString(read_file(opf_file))
        manifest_files = []
        manifest = opf.getElementsByTagName('manifest')[0]
        for el in manifest.getElementsByTagName('item'):
            f = el.getAttribute('href')
            f = os.path.normpath(f)
            manifest_files.append(f)
        count = 0
        for f in resource_files:
            if f not in manifest_files:
                count += 1
                verbose('adding to manifest: %s' % f)
                item = opf.createElement('item')
                item.setAttribute('href', f.replace(os.path.sep, '/'))
                item.setAttribute('id', 'a2x-%d' % count)
                mimetype = mimetypes.guess_type(f)[0]
                if mimetype is None:
                    die('unknown mimetype: %s' % f)
                item.setAttribute('media-type', mimetype)
                manifest.appendChild(item)
        if count > 0:
            write_file(opf_file, opf.toxml())

    def to_epub(self):
        self.to_docbook()
        xsl_file = self.xsl_stylesheet()
        docbook_file = self.dst_path('.xml')
        epub_file = self.dst_path('.epub')
        build_dir = epub_file + '.d'
        shell_rmtree(build_dir)
        shell_makedirs(build_dir)
        # Create content.
        exec_xsltproc(xsl_file, docbook_file, build_dir, self.xsltproc_opts)
        # Copy resources referenced in the OPF and resources referenced by the
        # generated HTML (in theory DocBook XSL should ensure they are
        # identical but this is not always the case).
        src_dir = os.path.dirname(self.asciidoc_file)
        dst_dir = os.path.join(build_dir, 'OEBPS')
        opf_file = os.path.join(dst_dir, 'content.opf')
        opf_resources = find_resources(opf_file, 'item', 'href')
        html_files = find_files(dst_dir, '*.html')
        self.copy_resources(html_files, src_dir, dst_dir, opf_resources)
        # Register any unregistered resources.
        self.update_epub_manifest(opf_file)
        # Build epub archive.
        cwd = os.getcwd()
        shell_cd(build_dir)
        try:
            if not self.dry_run:
                zip = zipfile.ZipFile(epub_file, 'w')
                try:
                    # Create and add uncompressed mimetype file.
                    verbose('archiving: mimetype')
                    write_file('mimetype', 'application/epub+zip')
                    zip.write('mimetype', compress_type=zipfile.ZIP_STORED)
                    # Compress all remaining files.
                    for (p,dirs,files) in os.walk('.'):
                        for f in files:
                            f = os.path.normpath(os.path.join(p,f))
                            if f != 'mimetype':
                                verbose('archiving: %s' % f)
                                zip.write(f, compress_type=zipfile.ZIP_DEFLATED)
                finally:
                    zip.close()
            verbose('created archive: %s' % epub_file)
        finally:
            shell_cd(cwd)
        if not self.keep_artifacts:
            shell_rmtree(build_dir)
        if self.epubcheck and EPUBCHECK:
            if not find_executable(EPUBCHECK):
                warning('epubcheck skipped: unable to find executable: %s' % EPUBCHECK)
            else:
                shell('"%s" "%s"' % (EPUBCHECK, epub_file))

    def to_text(self):
        text_file = self.dst_path('.text')
        html_file = self.dst_path('.text.html')
        if self.lynx:
            shell('"%s" %s --conf-file "%s" -b html4 -a "a2x-format=%s" -o "%s" "%s"' %
                 (self.asciidoc, self.asciidoc_opts, self.asciidoc_conf_file('text.conf'),
                  self.format, html_file, self.asciidoc_file))
            shell('"%s" -dump "%s" > "%s"' %
                 (LYNX, html_file, text_file))
        else:
            # Use w3m(1).
            self.to_docbook()
            docbook_file = self.dst_path('.xml')
            opts = '%s --output "%s"' % (self.xsltproc_opts, html_file)
            exec_xsltproc(self.xsl_stylesheet(), docbook_file,
                    self.destination_dir, opts)
            shell('"%s" -cols 70 -dump -T text/html -no-graph "%s" > "%s"' %
                 (W3M, html_file, text_file))
        if not self.keep_artifacts:
            shell_rm(html_file)


#####################################################################
# Script main line.
#####################################################################

if __name__ == '__main__':
    description = '''A toolchain manager for AsciiDoc (converts Asciidoc text files to other file formats)'''
    from optparse import OptionParser
    parser = OptionParser(usage='usage: %prog [OPTIONS] SOURCE_FILE',
        version='%s %s' % (PROG,VERSION),
        description=description)
    parser.add_option('-a', '--attribute',
        action='append', dest='attributes', default=[], metavar='ATTRIBUTE',
        help='set asciidoc attribute value')
    parser.add_option('--asciidoc-opts',
        action='append', dest='asciidoc_opts', default=[],
        metavar='ASCIIDOC_OPTS', help='asciidoc options')
    #DEPRECATED
    parser.add_option('--copy',
        action='store_true', dest='copy', default=False,
        help='DEPRECATED: does nothing')
    parser.add_option('--conf-file',
        dest='conf_file', default=None, metavar='CONF_FILE',
        help='configuration file')
    parser.add_option('-D', '--destination-dir',
        action='store', dest='destination_dir', default=None, metavar='PATH',
        help='output directory (defaults to SOURCE_FILE directory)')
    parser.add_option('-d','--doctype',
        action='store', dest='doctype', metavar='DOCTYPE',
        choices=('article','manpage','book'),
        help='article, manpage, book')
    parser.add_option('-b','--backend',
        action='store', dest='backend', metavar='BACKEND',
        help='name of backend plugin')
    parser.add_option('--epubcheck',
        action='store_true', dest='epubcheck', default=False,
        help='check EPUB output with epubcheck')
    parser.add_option('-f','--format',
        action='store', dest='format', metavar='FORMAT', default = 'pdf',
        choices=('chunked','epub','htmlhelp','manpage','pdf', 'text',
                 'xhtml','dvi','ps','tex','docbook'),
        help='chunked, epub, htmlhelp, manpage, pdf, text, xhtml, dvi, ps, tex, docbook')
    parser.add_option('--icons',
        action='store_true', dest='icons', default=False,
        help='use admonition, callout and navigation icons')
    parser.add_option('--icons-dir',
        action='store', dest='icons_dir',
        default=None, metavar='PATH',
        help='admonition and navigation icon directory')
    parser.add_option('-k', '--keep-artifacts',
        action='store_true', dest='keep_artifacts', default=False,
        help='do not delete temporary build files')
    parser.add_option('--lynx',
        action='store_true', dest='lynx', default=False,
        help='use lynx to generate text files')
    parser.add_option('-L', '--no-xmllint',
        action='store_true', dest='no_xmllint', default=False,
        help='do not check asciidoc output with xmllint')
    parser.add_option('-n','--dry-run',
        action='store_true', dest='dry_run', default=False,
        help='just print the commands that would have been executed')
    parser.add_option('-r','--resource',
        action='append', dest='resources', default=[],
        metavar='PATH',
        help='resource file or directory containing resource files')
    parser.add_option('-m', '--resource-manifest',
        action='store', dest='resource_manifest', default=None, metavar='FILE',
        help='read resources from FILE')
    #DEPRECATED
    parser.add_option('--resource-dir',
        action='append', dest='resources', default=[],
        metavar='PATH',
        help='DEPRECATED: use --resource')
    #DEPRECATED
    parser.add_option('-s','--skip-asciidoc',
        action='store_true', dest='skip_asciidoc', default=False,
        help='DEPRECATED: redundant')
    parser.add_option('--stylesheet',
        action='store', dest='stylesheet', default=None,
        metavar='STYLESHEET',
        help='HTML CSS stylesheet file name')
    #DEPRECATED
    parser.add_option('--safe',
        action='store_true', dest='safe', default=False,
        help='DEPRECATED: does nothing')
    parser.add_option('--dblatex-opts',
        action='append', dest='dblatex_opts', default=[],
        metavar='DBLATEX_OPTS', help='dblatex options')
    parser.add_option('--backend-opts',
        action='append', dest='backend_opts', default=[],
        metavar='BACKEND_OPTS', help='backend plugin options')
    parser.add_option('--fop',
        action='store_true', dest='fop', default=False,
        help='use FOP to generate PDF files')
    parser.add_option('--fop-opts',
        action='append', dest='fop_opts', default=[],
        metavar='FOP_OPTS', help='options for FOP pdf generation')
    parser.add_option('--xsltproc-opts',
        action='append', dest='xsltproc_opts', default=[],
        metavar='XSLTPROC_OPTS', help='xsltproc options for XSL stylesheets')
    parser.add_option('--xsl-file',
        action='store', dest='xsl_file', metavar='XSL_FILE',
        help='custom XSL stylesheet')
    parser.add_option('-v', '--verbose',
        action='count', dest='verbose', default=0,
        help='increase verbosity')
    if len(sys.argv) == 1:
        parser.parse_args(['--help'])
    source_options = get_source_options(sys.argv[-1])
    argv = source_options + sys.argv[1:]
    opts, args = parser.parse_args(argv)
    if len(args) != 1:
        parser.error('incorrect number of arguments')
    opts.asciidoc_opts = ' '.join(opts.asciidoc_opts)
    opts.dblatex_opts = ' '.join(opts.dblatex_opts)
    opts.fop_opts = ' '.join(opts.fop_opts)
    opts.xsltproc_opts = ' '.join(opts.xsltproc_opts)
    opts.backend_opts = ' '.join(opts.backend_opts)
    opts = eval(str(opts))  # Convert optparse.Values to dict.
    a2x = A2X(opts)
    OPTIONS = a2x           # verbose and dry_run used by utility functions.
    verbose('args: %r' % argv)
    a2x.asciidoc_file = args[0]
    try:
        a2x.load_conf()
        a2x.execute()
    except KeyboardInterrupt:
        exit(1)
