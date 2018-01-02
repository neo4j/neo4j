/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util.dbstructure;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.SecureClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.neo4j.function.Function;
import org.neo4j.helpers.collection.Visitable;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.function.Functions.constant;

public class DbStructureInvocationTracingAcceptanceTest
{
    private final String packageName = "org.neo4j.kernel.impl.util.data";
    private final String className = "XXYYZZData";
    private final String classNameWithPackage = packageName + "." + className;

    @Test
    public void outputCompilesWithoutErrors() throws IOException
    {
        // GIVEN
        StringBuilder output = new StringBuilder();
        InvocationTracer<DbStructureVisitor> tracer =
            new InvocationTracer<>( "Test", packageName, className, DbStructureVisitor.class, DbStructureArgumentFormatter.INSTANCE, output );
        DbStructureVisitor visitor = tracer.newProxy();

        // WHEN
        exerciseVisitor( constant( visitor ) );
        tracer.close();

        // THEN
        assertCompiles( classNameWithPackage, output.toString() );
    }

    @Test
    public void compiledOutputCreatesInputTrace() throws IOException
    {
        // GIVEN
        StringBuilder output = new StringBuilder();
        InvocationTracer<DbStructureVisitor> tracer =
            new InvocationTracer<>( "Test", packageName, className, DbStructureVisitor.class, DbStructureArgumentFormatter.INSTANCE, output );
        exerciseVisitor( constant ( tracer.newProxy() ) );
        tracer.close();
        final Visitable<DbStructureVisitor> visitable = compileVisitable( classNameWithPackage, output.toString() );
        final DbStructureVisitor visitor = mock( DbStructureVisitor.class );

        // WHEN
        visitable.accept( visitor );

        // THEN
        exerciseVisitor( new Function<Object, DbStructureVisitor>()
        {
            @Override
            public DbStructureVisitor apply( Object o ) throws RuntimeException
            {
                return verify( visitor );
            }
        } );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void compiledOutputProducesSameCompiledOutputIfCompiledAgain() throws IOException
    {
        // GIVEN
        StringBuilder output1 = new StringBuilder();
        InvocationTracer<DbStructureVisitor> tracer1 =
                new InvocationTracer<>( "Test", packageName, className, DbStructureVisitor.class, DbStructureArgumentFormatter.INSTANCE, output1 );
        DbStructureVisitor visitor1 = tracer1.newProxy();
        exerciseVisitor( constant( visitor1 ) );
        tracer1.close();
        String source1 = output1.toString();
        Visitable<DbStructureVisitor> visitable = compileVisitable( classNameWithPackage, source1 );

        // WHEN
        StringBuilder output2 = new StringBuilder();
        InvocationTracer<DbStructureVisitor> tracer2 =
            new InvocationTracer<>( "Test", packageName, className, DbStructureVisitor.class, DbStructureArgumentFormatter.INSTANCE, output2 );
        DbStructureVisitor visitor2 = tracer2.newProxy();
        visitable.accept( visitor2 );
        tracer2.close();
        String source2 = output2.toString();

        // THEN
        assertEquals( source1, source2 );
    }

    private void exerciseVisitor( Function<Object, DbStructureVisitor> visitor )
    {
        visitor.apply( null ).visitLabel( 0, "Person" );
        visitor.apply( null ).visitLabel( 1, "Party" );
        visitor.apply( null ).visitPropertyKey( 0, "name" );
        visitor.apply( null ).visitPropertyKey( 1, "age" );
        visitor.apply( null ).visitRelationshipType( 0, "ACCEPTS" );
        visitor.apply( null ).visitRelationshipType( 1, "REJECTS" );
        visitor.apply( null ).visitIndex( new IndexDescriptor( 0, 1 ), ":Person(age)", 0.5d, 1l );
        visitor.apply( null ).visitUniqueIndex( new IndexDescriptor( 0, 0 ), ":Person(name)", 0.5d, 1l );
        visitor.apply( null ).visitUniqueConstraint( new UniquenessConstraint( 1, 0 ), ":Party(name)" );
        visitor.apply( null ).visitAllNodesCount( 55 );
        visitor.apply( null ).visitNodeCount( 0, "Person", 50 );
        visitor.apply( null ).visitNodeCount( 0, "Party", 5 );
        visitor.apply( null ).visitRelCount( 0, 1, -1, "MATCH (:Person)-[:REJECTS]->() RETURN count(*)", 5 );
    }

    private void assertCompiles( final String className, String source )
    {
        compile( className, source,
                new CompilationListener<Boolean>()
                {
                    @Override
                    public Boolean compiled( Boolean success,
                                             JavaFileManager manager,
                                             List<Diagnostic<? extends JavaFileObject>> diagnostics )
                    {
                        assertSuccessfullyCompiled( success, diagnostics, className );
                        return true;
                    }
                }
        );
    }

    private Visitable<DbStructureVisitor> compileVisitable( final String className, String inputSource )
    {
        return compile( className, inputSource,
                new CompilationListener<Visitable<DbStructureVisitor>>()
                {
                    @Override
                    public Visitable<DbStructureVisitor> compiled( Boolean success, JavaFileManager manager,
                                                                   List<Diagnostic<? extends JavaFileObject>> diagnostics )
                    {
                        assertSuccessfullyCompiled( success, diagnostics, className );
                        Object instance;
                        try
                        {
                            ClassLoader classLoader = manager.getClassLoader( null );
                            Class<?> clazz = classLoader.loadClass( className );
                            instance = clazz.getDeclaredField( "INSTANCE" ).get( null );
                        }
                        catch ( IllegalAccessException | ClassNotFoundException | NoSuchFieldException e )
                        {
                            throw new AssertionError( "Failed to instantiate compiled class", e );
                        }
                        return (Visitable<DbStructureVisitor>) instance;
                    }
                }
        );
    }

    private void assertSuccessfullyCompiled( Boolean success,
                                             List<Diagnostic<? extends JavaFileObject>> diagnostics,
                                             String className )
    {
        if ( success == null || !success )
        {
            StringBuilder builder = new StringBuilder();
            builder.append( "Failed to compile: " );
            builder.append( className );
            builder.append( "\n\n" );
            for ( Diagnostic<?> diagnostic : diagnostics )
            {
                builder.append( diagnostic.toString() );
                builder.append( "\n" );
            }
            throw new AssertionError( builder.toString() );
        }
    }

    private <T> T compile( String className, String source, CompilationListener<T> listener )
    {
        JavaCompiler systemCompiler = ToolProvider.getSystemJavaCompiler();
        JavaFileManager manager = new InMemFileManager();
        DiagnosticCollector<JavaFileObject> diagnosticsCollector = new DiagnosticCollector<JavaFileObject>();
        Iterable<? extends JavaFileObject> sources = Arrays.asList( new InMemSource( className, source ) );
        CompilationTask task = systemCompiler.getTask( null, manager, diagnosticsCollector, null, null, sources );
        Boolean success = task.call();
        return listener.compiled( success, manager, diagnosticsCollector.getDiagnostics() );
    }

    private static interface CompilationListener<T>
    {
        T compiled( Boolean success, JavaFileManager manager, List<Diagnostic<? extends JavaFileObject>> diagnostics );
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

        public byte[] getBytes() {
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
        private final Map<String, InMemSink> classes = new HashMap<>();

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
                    byte[] byteCode = classes.get( name ).getBytes();
                    return super.defineClass( name, byteCode, 0, byteCode.length );
                }
            };
        }

        @Override
        public JavaFileObject getJavaFileForOutput( Location location, String className,
                                                    Kind kind, FileObject sibling ) throws IOException
        {
            if ( StandardLocation.CLASS_OUTPUT == location && Kind.CLASS == kind )
            {
                InMemSink clazz = new InMemSink( className );
                classes.put( className, clazz );
                return clazz;
            }
            else
            {
                return super.getJavaFileForOutput( location, className, kind, sibling );
            }
        }
    }
}
