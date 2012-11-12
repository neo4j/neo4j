# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Support for writing JUnit XML test results for the unit tests

The base of this code is borrowed from the Jython project.
"""

import os
import re
import sys
import time
import traceback
import unittest
from StringIO import StringIO
from xml.sax import saxutils

# Invalid XML characters (control chars)
EVIL_CHARACTERS_RE = re.compile(r"[\000-\010\013\014\016-\037]")

class JUnitXMLTestRunner:
    """A unittest runner that writes results to a JUnit XML file in
    xml_dir
    """

    def __init__(self, xml_dir):
        self.xml_dir = xml_dir

    def run(self, test):
        result = JUnitXMLTestResult(self.xml_dir)
        test(result)
        result.write_xml()
        return result


class TestList(list):
    def __init__(self):
        self.errors = 0
        self.failures = 0
        self.took = 0

class JUnitXMLTestResult(unittest.TestResult):
    """JUnit XML test result writer.

    The name of the file written to is determined from the full module
    name of the first test ran
    """

    def __init__(self, xml_dir):
        unittest.TestResult.__init__(self)
        self.xml_dir = xml_dir

        # The module name of the first test ran
        self.tests = {}

        # Start time
        self.start = None

        self.old_stdout = sys.stdout
        self.old_stderr = sys.stderr
        sys.stdout = self.stdout = Tee(sys.stdout)
        sys.stderr = self.stderr = Tee(sys.stderr)

    def startTest(self, test):
        unittest.TestResult.startTest(self, test)
        self.error, self.failure = None, None
        self.start = time.time()

    def stopTest(self, test):
        took = time.time() - self.start
        unittest.TestResult.stopTest(self, test)
        args = [test, took, self.stdout.getvalue(), self.stderr.getvalue()]
        self.stdout.truncate(0)
        self.stderr.truncate(0)
        
        testsuite = '.'.join(test.id().split('.')[:-1])
        if testsuite not in self.tests:
            self.tests[testsuite] = TestList()
        tests = self.tests[testsuite]
        tests.took += took
        
        if self.error:
            tests.errors += 1
            args.extend(['error', self.error])
        elif self.failure:
            tests.failures += 1
            args.extend(['failure', self.failure])
        tests.append(TestInfo.from_testcase(*args))

    def addError(self, test, err):
        unittest.TestResult.addError(self, test, err)
        self.error = err

    def addFailure(self, test, err):
        unittest.TestResult.addFailure(self, test, err)
        self.failure = err

    def write_xml(self):
        if not self.tests: # No tests ran, nothing to write
            return

        stdout = self.stdout.getvalue()
        stderr = self.stderr.getvalue()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

        ensure_dir(self.xml_dir)

        for module in self.tests:
            tests = self.tests[module]

            filename = os.path.join(self.xml_dir, 'TEST-%s.xml' % module)
            stream = open(filename, 'w')

            write_testsuite_xml(stream, len(tests), tests.errors,
                                tests.failures, 0, module, tests.took)

            for info in tests:
                info.write_xml(stream)

            stream.write('</testsuite>\n')
            stream.close()


class TestInfo(object):

    """The JUnit XML <testcase/> model."""

    def __init__(self, name, took, type, exc_info, stdout='', stderr=''):
        # The name of the test
        self.name = name

        # How long it took
        self.took = took

        # Type of test: 'error', 'failure' 'skipped', or None for a success
        self.type = type

        self.stdout = stdout
        self.stderr = stderr

        if exc_info:
            self.exc_name = exc_name(exc_info)
            self.message = exc_message(exc_info)
            self.traceback = safe_str(''.join(
                    traceback.format_exception(*exc_info)))
        else:
            self.exc_name = self.message = self.traceback = ''

    @classmethod
    def from_testcase(cls, testcase, took, out, err, type=None, exc_info=None):
        name = testcase.id().split('.')[-1]
        return cls(name, took, type, exc_info, out, err)
    
    def write_xml(self, stream):
        stream.write('  <testcase name="%s" time="%.3f"' % (self.name,
                                                            self.took))

        if not (self.type or self.stdout or self.stderr):
            # test was successful
            stream.write('/>\n')
            return
        stream.write('>\n')
        if self.type:
            stream.write('    <%s type="%s" message=%s><![CDATA[%s]]></%s>\n' %
                         (self.type, self.exc_name,
                          saxutils.quoteattr(self.message),
                          escape_cdata(self.traceback), self.type))
        write_stdouterr_xml(stream, self.stdout, self.stderr)
        stream.write('  </testcase>\n')


class Tee(StringIO):

    """Writes data to this StringIO and a separate stream"""

    def __init__(self, stream):
        StringIO.__init__(self)
        self.stream = stream

    def write(self, data):
        StringIO.write(self, data)
        self.stream.write(data)

    def flush(self):
        StringIO.flush(self)
        self.stream.flush()


def write_testsuite_xml(stream, tests, errors, failures, skipped, name, took):
    """Write the XML header (<testsuite/>)"""
    stream.write('<?xml version="1.0" encoding="utf-8"?>\n')
    stream.write('<testsuite tests="%d" errors="%d" failures="%d" ' %
                 (tests, errors, failures))
    stream.write('skipped="%d" name="%s" time="%.3f">\n' % (skipped, name,
                                                            took))

def write_stdouterr_xml(stream, stdout, stderr):
    """Write the stdout/err tags"""
    if stdout:
        stream.write('    <system-out><![CDATA[%s]]></system-out>\n' %
                     escape_cdata(safe_str(stdout)))
    if stderr:
        stream.write('    <system-err><![CDATA[%s]]></system-err>\n' %
                     escape_cdata(safe_str(stderr)))


def ensure_dir(dir):
    """Ensure dir exists"""
    if dir.endswith('/'): dir = dir[:-1]
    if not os.path.exists(dir):
        ensure_dir(os.path.dirname(dir))
        os.mkdir(dir)


def exc_name(exc_info):
    """Determine the full name of the exception that caused exc_info"""
    exc = exc_info[1]
    name = getattr(exc.__class__, '__module__', '')
    if name:
        name += '.'
    return name + exc.__class__.__name__


def exc_message(exc_info):
    """Safely return a short message passed through safe_str describing
    exc_info, being careful of unicode values.
    """
    exc = exc_info[1]
    if exc is None:
        return safe_str(exc_info[0])
    if isinstance(exc, BaseException) and isinstance(exc.message, unicode):
        return safe_str(exc.message)
    try:
        return safe_str(str(exc))
    except UnicodeEncodeError:
        try:
            val = unicode(exc)
            return safe_str(val)
        except UnicodeDecodeError:
            return '?'


def escape_cdata(cdata):
    """Escape a string for an XML CDATA section"""
    return cdata.replace(']]>', ']]>]]&gt;<![CDATA[')


def safe_str(base):
    """Return a str valid for UTF-8 XML from a basestring"""
    if isinstance(base, unicode):
        return remove_evil(base.encode('utf-8', 'replace'))
    return remove_evil(base.decode('utf-8', 'replace').encode('utf-8',
                                                              'replace'))


def remove_evil(string):
    """Remove control characters from a string"""
    return EVIL_CHARACTERS_RE.sub('?', string)
