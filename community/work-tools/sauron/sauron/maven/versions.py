from xml.dom.minidom import parse
from deprep.report import report_deprecations
import sys

def determine_project_version():
    ''' Determine the project version of the current directory, assuming there is a pom.xml present.
    '''
    pom = parse('pom.xml')
    for vnode in pom.getElementsByTagName('version'):
        if vnode.parentNode == pom.documentElement:
            break
    else:
        raise Exception("Unable to determine project version")

    return vnode.childNodes[0].data

def subtract_version(version, to_subtract):
    ''' Naive method for subtracting versions
    '''
    version = version.lower().replace('-snapshot','').split('m')[0].split('r')[0].split('g')[0]

    parts = version.split('.')

    major = int(parts[0])
    minor = int(parts[1]) if len(parts) > 0 else 0

    if minor >= to_subtract:
        minor = minor - to_subtract
        return '{major}.{minor}'.format(major=major, minor=minor)
    else:
        raise Exception('Unable to figure out what {0} versions back from {1} is. Would need a listing of all previous versions to do that. Please improve me so that I can!'.format(to_subtract, version))
