/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.proc;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.neo4j.collection.PrefetchingRawIterator;
import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.logging.Log;

/**
 * Given the location of a jarfile, reads the contents of the jar and returns compiled {@link CallableProcedure}
 * instances.
 */
public class ProcedureJarLoader
{
    private final ReflectiveProcedureCompiler compiler;
    private final Log log;

    ProcedureJarLoader( ReflectiveProcedureCompiler compiler, Log log )
    {
        this.compiler = compiler;
        this.log = log;
    }

    public Callables loadProceduresFromDir( File root ) throws IOException, KernelException
    {
        if ( root == null || !root.exists() )
        {
            return Callables.empty();
        }

        Callables out = new Callables();

        File[] dirListing = root.listFiles( ( dir, name ) -> name.endsWith( ".jar" ) );

        if ( dirListing == null )
        {
            return Callables.empty();
        }

        if ( !allJarFilesAreValidZipFiles( Stream.of( dirListing ) ) )
        {
            throw new ZipException( "Some jar procedure files are invalid, see log for details." );
        }

        URL[] jarFilesURLs = Stream.of( dirListing ).map( this::toURL ).toArray( URL[]::new );

        URLClassLoader loader = new URLClassLoader( jarFilesURLs, this.getClass().getClassLoader() );

        for ( URL jarFile : jarFilesURLs )
        {
            loadProcedures( jarFile, loader, out );
        }
        return out;
    }

    private boolean allJarFilesAreValidZipFiles( Stream<File> jarFiles )
    {
        return jarFiles.allMatch( jarFile ->
        {
            try
            {
                new ZipFile( jarFile ).close();
                return true;
            }
            catch ( IOException e )
            {
                log.error( String.format( "Plugin jar file: %s corrupted.", jarFile ) );
                return false;
            }
        } );
    }

    private Callables loadProcedures( URL jar, ClassLoader loader, Callables target )
            throws IOException, KernelException
    {
        RawIterator<Class<?>,IOException> classes = listClassesIn( jar, loader );
        while ( classes.hasNext() )
        {
            Class<?> next = classes.next();
            target.addAllProcedures( compiler.compileProcedure( next, null, false ) );
            target.addAllFunctions( compiler.compileFunction( next ) );
            target.addAllAggregationFunctions( compiler.compileAggregationFunction( next ) );
        }
        return target;
    }

    private URL toURL( File f )
    {
        try
        {
            return f.toURI().toURL();
        }
        catch ( MalformedURLException e )
        {
            throw new RuntimeException( e );
        }
    }

    private RawIterator<Class<?>,IOException> listClassesIn( URL jar, ClassLoader loader ) throws IOException
    {
        ZipInputStream zip = new ZipInputStream( jar.openStream() );

        return new PrefetchingRawIterator<Class<?>,IOException>()
        {
            @Override
            protected Class<?> fetchNextOrNull() throws IOException
            {
                try
                {
                    while ( true )
                    {
                        ZipEntry nextEntry = zip.getNextEntry();
                        if ( nextEntry == null )
                        {
                            zip.close();
                            return null;
                        }

                        String name = nextEntry.getName();
                        if ( name.endsWith( ".class" ) )
                        {
                            String className =
                                    name.substring( 0, name.length() - ".class".length() ).replace( '/', '.' );

                            try
                            {
                                Class<?> aClass = loader.loadClass( className );
                                // We do getDeclaredMethods to trigger NoClassDefErrors, which loadClass above does
                                // not do.
                                // This way, even if some of the classes in a jar cannot be loaded, we still check
                                // the others.
                                aClass.getDeclaredMethods();
                                return aClass;
                            }
                            catch ( UnsatisfiedLinkError | NoClassDefFoundError | Exception e )
                            {
                                log.warn( "Failed to load `%s` from plugin jar `%s`: %s", className, jar.getFile(),
                                        e.getMessage() );
                            }
                        }
                    }
                }
                catch ( IOException | RuntimeException e )
                {
                    zip.close();
                    throw e;
                }
            }
        };
    }

    public static class Callables
    {
        private final List<CallableProcedure> procedures = new ArrayList<>();
        private final List<CallableUserFunction> functions = new ArrayList<>();
        private final List<CallableUserAggregationFunction> aggregationFunctions = new ArrayList<>();

        public void add( CallableProcedure proc )
        {
            procedures.add( proc );
        }

        public void add( CallableUserFunction func )
        {
            functions.add( func );
        }

        public List<CallableProcedure> procedures()
        {
            return procedures;
        }

        public List<CallableUserFunction> functions()
        {
            return functions;
        }

        public List<CallableUserAggregationFunction> aggregationFunctions()
        {
            return aggregationFunctions;
        }

        void addAllProcedures( List<CallableProcedure> callableProcedures )
        {
            procedures.addAll( callableProcedures );
        }

        void addAllFunctions( List<CallableUserFunction> callableFunctions )
        {
            functions.addAll( callableFunctions );
        }

        public void addAllAggregationFunctions( List<CallableUserAggregationFunction> callableFunctions )
        {
            aggregationFunctions.addAll( callableFunctions );
        }

        private static Callables EMPTY = new Callables();

        public static Callables empty()
        {
            return EMPTY;
        }
    }
}
