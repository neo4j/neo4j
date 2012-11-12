#!/bin/sh
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

""":"
if [ -z "$PYTHON" ]; then
    PYTHON=python
fi

# If Neo4j Python bindings are installed: use the installed ones
if ! $PYTHON -c "import neo4j" &> /dev/null; then
    # Otherwise: set up PYTHONPATH to use the checked out source
    SRC=$0
    for (( c=3; c>0; c-- )); do
        while [ -L "$SRC" ]; do
            SRC=$(readlink $SRC)
        done
        SRC=$(cd $(dirname $SRC); pwd)
    done

    if [ -z "$PYTHONPATH" ]; then
        PYTHONPATH="$SRC/main/python"
    else
        PYTHONPATH="$PYTHONPATH:$SRC/main/python"
    fi
    export PYTHONPATH

    if [ -z "$JYTHONPATH" ]; then
        JYTHONPATH="$SRC/main/python"
    else
        JYTHONPATH="$JYTHONPATH:$SRC/main/python"
    fi
    export JYTHONPATH
    
    NEO4J_PYTHON_CLASSPATH=$($SRC/bin/classpath)
    if [ $? -ne 0 ]; then exit -1; fi
    export NEO4J_PYTHON_CLASSPATH
fi

$PYTHON $0 "$@"
exit $?
":"""

__all__ = ()

import unittest, doctest, sys, os, traceback, junit_xml

if __name__ == '__main__':

    params = {'--classpath':None, '--junit':None}
    key = arg = None
    args = []
    for arg in sys.argv:
        if key is not None:
            params[key] = arg
            key = None
        elif arg.lower() in params:
            key = arg.lower()
        else:
            args.append(arg)

    if params['--classpath']:
        try:
            import java
        except:
            os.environ['NEO4J_PYTHON_CLASSPATH'] = params['--classpath']
        else:
            sys.path.extend(params['--classpath'].split(':'))
    if params['--junit']:
        runner = junit_xml.JUnitXMLTestRunner(params['--junit'])
    else:
        runner = None
    del key, arg, params
        
    modules = {}

    for candidate in os.listdir(os.path.dirname(os.path.abspath(__file__))):
        if candidate.endswith('.py'):
            candidate = candidate[:-3]
            try:
                exec("from %s import *" % candidate)
            except:
                modules[candidate] = traceback.format_exc()
            else:
                modules[candidate] = None

    class ImportTestModules(unittest.TestCase):
        for _module in modules:
            if modules[_module] is None:
                def _test(self):
                    pass
            else:
                def _test(self,name=_module,failure=modules[_module]):
                    sys.stderr.write(failure)
                    self.fail('Failed to import test module "%s"' % name)
            exec("test_import_%s = _test" % _module)

    class TestSuiteContainer(unittest.TestCase):
        pass

    def DocTestCases(module):
        try:
            return type('doctest_%s' % module, (TestSuiteContainer,),
                    {'__module__': module,
                     'test_suite': doctest.DocTestSuite(module)})
        except:
            def suite(self,failure=traceback.format_exc()):
                sys.stderr.write(str(failure))
                self.fail('Failed to get doctests for "%s"' % (module,))
            return type('doctest_%s' % module, (unittest.TestCase,),
                        {'__module__': module,
                         'test_suite': suite})

    class CustomTestLoader(unittest.TestLoader):
        def loadTestsFromTestCase(self, testCaseClass):
            if issubclass(testCaseClass, TestSuiteContainer)\
                    and testCaseClass is not TestSuiteContainer:
                return testCaseClass.test_suite
            return unittest.TestLoader.loadTestsFromTestCase(self,testCaseClass)

    neo4j_doctest = DocTestCases('neo4j')

    params = {'argv':args, 'testLoader':CustomTestLoader()}
    if runner is not None: params['testRunner'] = runner
    unittest.main(**params)

else: # imported as a module

    import neo4j

    class GraphDatabaseTest(unittest.TestCase):
        def setUp(self):
            testcase = type(self)
            for case in dir(testcase):
                if case.startswith('test_'): break
            else:
                return
            dirname, join = os.path.dirname, os.path.join
            path = dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))
            path = join(path,'target','testdata',testcase.__module__)
            
            if os.path.exists(path):
              import shutil
              shutil.rmtree(path)
            path = join(path,testcase.__name__)
            
            self.graphdb = neo4j.GraphDatabase(path)
        def tearDown(self):
            graphdb = getattr(self,'graphdb',None)
            if graphdb is not None:
                graphdb.shutdown()
