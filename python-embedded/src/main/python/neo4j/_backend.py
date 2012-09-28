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

"""Neo4j backend selection for Python."""

import sys, inspect
#, 'FinalTraversalBranch','AsOneStartBranch', 
NEO4J_JAVA_CLASSES = (
    ('org.neo4j.kernel.impl.core',      ('NodeProxy', 'RelationshipProxy',)),
    ('org.neo4j.kernel',                ('Uniqueness', 'Traversal', 'EmbeddedGraphDatabase',\
                                         'BidirectionalTraversalBranchPath','ExtendedPath',)),
    ('org.neo4j.kernel.impl.traversal', ('TraversalDescriptionImpl', 'TraverserImpl',\
                                         'TraversalBranchImpl', 'FinalTraversalBranch',\
                                         'AsOneStartBranch', 'StartNodeTraversalBranch',)),
    ('org.neo4j.kernel.impl.util',      ('SingleNodePath',)),
    ('org.neo4j.graphdb',               ('Direction', 'DynamicRelationshipType', 'PropertyContainer',\
                                         'Transaction', 'GraphDatabaseService', 'Node', 'Relationship',\
                                         'Path', 'NotFoundException',)),
    ('org.neo4j.graphdb.traversal',     ('Evaluation', 'Evaluator',)),
    ('org.neo4j.graphdb.index',         ('Index', 'IndexHits',)),
    ('org.neo4j.helpers.collection',    ('IterableWrapper',)),
    ('org.neo4j.cypher.javacompat',     ('ExecutionEngine',)),
    #('com.tinkerpop.blueprints.pgm.impls.neo4j', ('Neo4jGraph', 'Neo4jEdge', 'Neo4jVertex')),
    #('com.tinkerpop.gremlin', ('Gremlin')),
    ('java.util',                       ('HashMap',)),
)	

module = sys.modules[__name__]

def extends(CLASS):
    ## This lets us extend java classes "in place",
    ## adding methods and attributes to the original
    ## java classes, using the normal python class
    ## system as a DSL.
     
    class ParentClass(type):
        def __new__(Class, name, bases, body):
            if bases == ():
                return type.__new__(Class, name, (object,), body)
            else:
                for key, value in body.items():
                    if key not in ('__module__','__new__'):
                        if hasattr(CLASS, key):
                            setattr(CLASS, "_super__%s" % key, getattr(CLASS, key))
                        setattr(CLASS, key, value)
                return type(name, (object,), body)
    
    return ParentClass(getattr(CLASS,'__name__','Class'),(),{})

if sys.version_info >= (3,):
    class Type(type):
        def __new__(Type, name, bases, body):
            if '__metaclass__' in body:
                return body['__metaclass__'](name, bases, body)
            else:
                return type.__new__(Type, name, bases, body)
    Object = Type('Object', (object,), {})
    del Type # to get a consistent namespace regardless of version
    strings = str,
    integers = int,
else:
    Object = object
    strings = str, unicode
    integers = int, long


try:
    import java
except: # this isn't jython (and doesn't have the java module)
    import jpype, os

    def get_jvm_args():
        
        # Classpath set by environment var
        classpath = os.getenv('NEO4J_PYTHON_CLASSPATH',None)
        if classpath is None:
            # Classpath set by finding bundled jars
            jars = []
            from pkg_resources import resource_listdir, resource_filename
            for name in resource_listdir(__name__, 'javalib'):
                if name.endswith('.jar'):
                    jars.append(resource_filename(__name__, "javalib/%s" % name))
            if len(jars) > 0:
                divider = ';' if sys.platform == "win32" else ':'
                classpath = divider.join(jars)
            else:
                # Last resort
                classpath = '.'

        jvmargs = os.getenv('NEO4J_PYTHON_JVMARGS',"").split(" ")
        jvmargs = jvmargs + ['-Djava.class.path=' + classpath]
        
        if os.getenv('DEBUG',None) is "true":
            jvmargs = jvmargs + ['-Xdebug', '-Xnoagent', '-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000']
        
        return jvmargs
    
    def get_jvm_path():
        jvm = jpype.getDefaultJVMPath()
        if sys.platform == "win32" and (jvm is None or not os.path.exists(jvm)):
            # JPype does not always find java correctly
            # on windows, try using JAVA_HOME to detect it.
	          rootJre = os.getenv("JAVA_HOME", "c:/Program Files/Java/jre6")
	          if os.path.exists(rootJre+"/bin/javac.exe") :
		            # this is a JDK home
		            rootJre += '/jre'
	
	          for i in ['/bin/client/jvm.dll','/bin/server/jvm.dll']:
	              if os.path.exists(rootJre + i) :
		                jvm = rootJre + i
        
        if jvm is None:
            if os.getenv("JAVA_HOME", None) != None:
                raise IOError("Unable to find a java runtime to use. JAVA_HOME is set to '%s', but I could not find a JVM to use there." % os.getenv("JAVA_HOME"))
            else:
                raise IOError("Unable to find a java runtime to use. Please set JAVA_HOME to point to the folder that contains your jre or jdk.")
        return jvm
        
    jvm_path = get_jvm_path()
    jvm_args = get_jvm_args()

    try:
      jpype.startJVM(jvm_path, *jvm_args)
    except Exception, e:
      raise Exception("Unable to start JVM, even though I found the JVM path. If you are using windows, this may be due to missing system DLL files, please see the windows installation instructions in the neo4j documentation.",e)
      
    
    isStatic = jpype.JPackage("java.lang.reflect").Modifier.isStatic
      
    
    def _add_jvm_connection_boilerplate_to_class(CLASS):
        ''' In order for JPype to work in a threaded
        environment, each time we're in a new thread, 
        the method jpype.attachThreadToJVM() needs to 
        be called.
        
        This wraps all methods in a java class with
        boilerplate to check if the current thread
        is connected, and to connect it if that is
        not the case.
        '''
        def add_jvm_connection_boilerplate(fn):
            def decorator(*args,**kwargs):
                if not jpype.isThreadAttachedToJVM():
                    jpype.attachThreadToJVM()
                return fn(*args, **kwargs)
            return decorator
        
        statics = []
        for m in CLASS.__javaclass__.getMethods():
            if isStatic(m.getModifiers()):
                statics.append(m.getName())
        
        for key, val in inspect.getmembers(CLASS):
            if not key.startswith("__") and hasattr(val,'__call__'):
                wrapped = add_jvm_connection_boilerplate(val)
                if key in statics:
                    wrapped = staticmethod(wrapped)
                setattr(CLASS, key, wrapped)
    
    # Import java classes
    for pkg_name, class_names in NEO4J_JAVA_CLASSES:
        package = jpype.JPackage(pkg_name)
        for class_name in class_names:
            cls = getattr(package,class_name)
            _add_jvm_connection_boilerplate_to_class(cls)
            globals()[class_name] = cls
    
    
    # If JPype cannot find a class, it returns
    # package instances. Make sure we were able to load
    # classes.
    if isinstance(GraphDatabaseService, jpype.JPackage):
        raise ImportError("Cannot find Neo4j java classes, used classpath: %s" % classpath)

    def java_type(obj):
        java = jpype.java.lang
        conversions = (
            (strings, java.String),
            (bool,    java.Boolean),
            (integers,java.Long),
        )
        for pyType, jType in conversions:
            if isinstance(obj, pyType):
                return jType
        raise TypeError("I don't know how to convert this to a java primitive: %s" % repr(obj))

    def from_java(value):
        global from_java
        java = jpype.java.lang
        floats = (java.Double, java.Float)
        integers = (java.Long, java.Integer, java.Short, java.Byte)
        def from_java(value):
            if isinstance(value, floats):
                return value.doubleValue()
            if isinstance(value, integers):
                return value.longValue()
            if isinstance(value, java.Boolean):
                return True if value == True else False
            if isinstance(value, jpype._jarray._JavaArrayClass):
                return list(value)
            return value
        return from_java(value)
        
    def to_java(value):
        if isinstance(value, dict):
            pyDict = value
            value = HashMap()
            for k,v in pyDict.items():
                value.put(k,v)
        elif isinstance(value, list):
            if len(value) == 0:
                raise TypeError('Empty lists are not supported, since we cannot determine what type of list they should be in Java.')
            pyList = value
            JavaType = java_type(pyList[0])
            value = jpype.JArray(JavaType)(len(pyList))
            for i in range(len(pyList)):
                value[i] = JavaType(pyList[i])
        return value
        
    def implements(interface):
        class InterfaceProxy(object):
            def __new__(cls, *args, **kwargs):
                inst = super(InterfaceProxy, cls).__new__(cls, *args, **kwargs)
                inst.__init__(*args, **kwargs)
                return jpype.JProxy((interface,), inst=inst)
        return InterfaceProxy
        
    def create_embedded_db(*args):
        return EmbeddedGraphDatabase(*args)
    
      
else:
    # Import java classes
    for pkg_name, class_names in NEO4J_JAVA_CLASSES:
        imports = __import__(pkg_name, globals(), locals(), class_names,-1)
        for class_name in class_names:
            globals()[class_name] = getattr(imports,class_name)
    
    def from_java(value):
        return value
    
    def to_java(value):
        return value
        
    def implements(interface):
        return interface
        

rel_type = DynamicRelationshipType.withName
