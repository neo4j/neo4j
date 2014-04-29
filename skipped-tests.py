#!/usr/bin/env python
"""
Processes JUnit XML files and reports skipped tests.

A skipped test is a test marked as skipped in the JUnit XML, where the
message does not start with 'not a test' (case insensitive), or a date
in the format 'yyyy-mm-dd', and that date is less than the specified
number of days (the '--days' command line argument) (default=30).

Command line arguments:
--fail         - causes the program to exit with a non-zero exit code
                 if there were any skipped tests.
--days <days>  - the number of days ago that marks the limit for when
                 a test is old and should be updated rather than
                 skipped.
"""
import sys, getopt, os, re, datetime, xml.etree.ElementTree as ET
DATE = re.compile(r"^(?P<y>[\d]{4})-(?P<m>[\d]{2})-(?P<d>[\d]{2})(?:| )")

def parseOpts(args):
    opts, args = getopt.getopt(args, '', ['days=','fail'])
    result = {'days':30,'fail':False}
    for o, a in opts:
        if o == '--days':
            result['days'] = int(a)
        elif o == 'fail':
            result['fail'] = True
    return result

def each(it): return it

def skippedTests(basedir, expirationAge):
    for (dirpath, dirs, files) in os.walk(os.curdir):
        for testXml in each(os.path.join(dirpath,f) for f in files
                            if f.startswith("TEST-") and f.endswith(".xml")):
            testcases = ET.parse(testXml).findall("./testcase")
            if testcases is None: continue
            for testcase in testcases:
                skipped = testcase.find("./skipped")
                if skipped is None: continue
                message = skipped.get("message","NO MESSAGE!")
                if message.lower().startswith("not a test"): continue
                match = DATE.search(message)
                if match:
                    y,m,d = match.group('y'),match.group('m'),match.group('d')
                    date = datetime.date(int(y),int(m),int(d))
                    delta = datetime.date.today() - date
                    if delta < expirationAge: continue
                    message = "Expired %s days ago! %s"%(delta.days,message)
                classname = testcase.get("classname","")
                testname = testcase.get("name","")
                yield {'class':classname,'test':testname,'message':message}

if __name__ == '__main__':
    opts = parseOpts(sys.argv)
    expirationAge = datetime.timedelta(days=opts['days'])
    fail = False

    for skipped in skippedTests(os.curdir, expirationAge):
        fail = True
        if skipped['test']==skipped['class']:
            print "Skipped test case %(test)s: %(message)s"%skipped
        else:
            print "Skipped test case %(class)s#%(test)s: %(message)s"%skipped
            
    if fail and opts['fail']:
        sys.exit(-1)
    

