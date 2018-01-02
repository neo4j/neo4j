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
package org.neo4j.visualization.graphviz;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.walk.Walker;

public class Script extends ConfigurationParser
{
    public Script( File config, String... format )
    {
        super( config, format );
    }

    public Script( String... format )
    {
        super( format );
    }

    protected String storeDir;

    public static <S extends Script> S initialize( Class<S> scriptClass, String... args )
    {
        if ( args.length < 1 )
        {
            throw new IllegalArgumentException(
                    "GraphvizWriter expects at least one  argument, the path "
                            + "to the Neo4j storage dir." );
        }
        String[] format = new String[args.length - 1];
        System.arraycopy( args, 1, format, 0, format.length );
        String configFile = System.getProperty( "org.neo4j.visualization.ConfigFile", null );
        S script = null;
        try
        {
            Constructor<S> ctor;
            if ( configFile != null ) // Try invoking with configuration file
            {
                try
                {
                    ctor = scriptClass.getConstructor( File.class, String[].class );
                    script = ctor.newInstance( new File( configFile ), format );
                }
                catch ( NoSuchMethodException handled )
                {
                    if ( format == null || format.length == 0 )
                    {
                        try
                        {
                            ctor = scriptClass.getConstructor( File.class );
                            script = ctor.newInstance( new File( configFile ) );
                        }
                        catch ( NoSuchMethodException fallthrough )
                        {
                            // Ignore
                        }
                    }
                }
            }
            if ( script == null ) // Not created with configuration file
            {
                try
                {
                    ctor = scriptClass.getConstructor( String[].class );
                    script = ctor.newInstance( (Object) format );
                }
                catch ( NoSuchMethodException exception )
                {
                    if ( format == null || format.length == 0 )
                    {
                        script = scriptClass.newInstance();
                    }
                    else
                    {
                        throw exception;
                    }
                }
            }
        }
        catch ( NoSuchMethodException e )
        {
            throw new UnsupportedOperationException( scriptClass.getName()
                                                     + " does not have a suitable constructor", e );
        }
        catch ( InvocationTargetException e )
        {
            throw new UnsupportedOperationException( "Could not initialize script",
                    e.getTargetException() );
        }
        catch ( Exception e )
        {
            throw new UnsupportedOperationException( "Could not initialize script", e );
        }
        script.storeDir = args[0];
        return script;
    }

    public static void main( Class<? extends Script> scriptClass, String... args )
    {
        initialize( scriptClass, args ).emit(
                new File( System.getProperty( "graphviz.out", "graph.dot" ) ) );
    }

    /**
     * @param args The command line arguments.
     */
    public static void main( String... args )
    {
        main( Script.class, args );
    }

    public final void emit( File outfile )
    {
        GraphDatabaseService graphdb = createGraphDb();
        GraphvizWriter writer = new GraphvizWriter( styles() );
        try
        {
            try ( Transaction tx = graphdb.beginTx() )
            {
                writer.emit( outfile, createGraphWalker( graphdb ) );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    protected String storeDir()
    {
        return storeDir;
    }

    protected GraphDatabaseService createGraphDb()
    {
        return new GraphDatabaseFactory().newEmbeddedDatabase( storeDir() );
    }

    protected Walker createGraphWalker( GraphDatabaseService graphdb )
    {
        return Walker.fullGraph( graphdb );
    }
}
