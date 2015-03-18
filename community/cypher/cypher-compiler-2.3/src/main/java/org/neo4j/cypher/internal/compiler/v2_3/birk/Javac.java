/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_3.birk;

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.Statement;
import sun.tools.java.CompilerError;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.security.SecureClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.tools.*;

import static javax.tools.JavaCompiler.CompilationTask;

//TODO this should be replaced, here for testing stuff out
public class Javac
{
    //TODO this is not the way it should work…
    @SuppressWarnings( "unchecked" )
    public static InternalExecutionResult newInstance( String className, String classBody, Statement statement, GraphDatabaseService db ) throws Exception
    {
       return newInstance( compile(className, classBody), statement, db );
    }

    public static Class<InternalExecutionResult> compile(String className, String classBody) throws
            ClassNotFoundException
    {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager manager = new InMemFileManager();
        DiagnosticCollector<JavaFileObject> diagnosticsCollector = new DiagnosticCollector<>();
        Iterable<? extends JavaFileObject> sources = Arrays.asList( new InMemSource( className, classBody ) );
        CompilationTask task = compiler.getTask( null, manager, diagnosticsCollector, null, null, sources );

        if ( !task.call() )
        {
            StringBuilder sb = new StringBuilder();
            int number = 1;
            for ( Diagnostic<?> diagnostic : diagnosticsCollector.getDiagnostics() )
            {
                sb.append(  diagnostic.getKind() + "  : " + number + " Type : " + diagnostic.getMessage(Locale.getDefault()));
                sb.append(" at column : " + diagnostic.getColumnNumber() );
                sb.append(" Line number : " + diagnostic.getLineNumber() + System.lineSeparator() );
                number++;
            }
            throw new CompilerError( sb.toString() );
        }

        Class<InternalExecutionResult> clazz = (Class<InternalExecutionResult>) manager.getClassLoader( null ).loadClass(className );
        return clazz;
    }

    public static InternalExecutionResult newInstance(Class<InternalExecutionResult> clazz, Statement statement, GraphDatabaseService db) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException
    {
        Constructor<InternalExecutionResult> constructor = clazz.getDeclaredConstructor( Statement.class, GraphDatabaseService.class );
        return  constructor.newInstance( statement, db );
    }

    private static class InMemSource extends SimpleJavaFileObject
    {
        final String javaSource;

        InMemSource( String className, String javaSource )
        {
            super( URI.create( "string:///" + className.replace( '.', '/' ) + Kind.SOURCE.extension ), Kind.SOURCE );
            this.javaSource = javaSource;
        }

        @Override
        public CharSequence getCharContent( boolean ignoreEncodingErrors )
        {
            return javaSource;
        }
    }

    private static class InMemSink extends SimpleJavaFileObject
    {
        private ByteArrayOutputStream byteCodeStream = new ByteArrayOutputStream();

        InMemSink( String className )
        {
            super( URI.create( "mem:///" + className + Kind.CLASS.extension ), Kind.CLASS );
        }

        public byte[] getBytes()
        {
            return byteCodeStream.toByteArray();
        }

        @Override
        public OutputStream openOutputStream() throws IOException
        {
            return byteCodeStream;
        }
    }

    private static class InMemFileManager extends ForwardingJavaFileManager
    {
        private final Map<String,InMemSink> classNameToByteCode = new HashMap<>();

        InMemFileManager()
        {
            super( ToolProvider.getSystemJavaCompiler().getStandardFileManager( null, null, null ) );
        }

        @Override
        public ClassLoader getClassLoader( Location location )
        {
            return new SecureClassLoader()
            {
                @Override
                protected Class<?> findClass( String name ) throws ClassNotFoundException
                {
                    byte[] byteCode = classNameToByteCode.get( name ).getBytes();
                    return super.defineClass( name, byteCode, 0, byteCode.length );
                }
            };
        }

        @Override
        public JavaFileObject getJavaFileForOutput( Location location, String className,
                JavaFileObject.Kind kind, FileObject sibling ) throws IOException
        {
            if ( StandardLocation.CLASS_OUTPUT == location && JavaFileObject.Kind.CLASS == kind )
            {
                InMemSink clazz = new InMemSink( className );
                classNameToByteCode.put( className, clazz );
                return clazz;
            }
            else
            {
                return super.getJavaFileForOutput( location, className, kind, sibling );
            }
        }
    }
}
