/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.neo4j.kernel.Config.*;

/**
 * Standalone EmbeddedGraphDatabase with Shell enabled.
 */
public class StandaloneWithShell
{
    private static Logger log = Logger.getLogger( StandaloneWithShell.class.getName() );
    private GraphDatabaseService embeddedDb;
    private AtomicBoolean shutdownInitiated = new AtomicBoolean( false );

    private GraphDatabaseService getGraphDb()
    {
        return this.embeddedDb;
    }

    private void addShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }

    private void initialize( Map<String, String> arguments )
    {
        String path = arguments.get( "path" );
        this.embeddedDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( path ).setConfig( ENABLE_REMOTE_SHELL, "true" ).newGraphDatabase();
        log.info( "Neo4j started at '" + path + "'" );
    }

    private void blockUntilShutdown()
    {
        try
        {
            while ( shutdownInitiated.get() == false )
            {
                Thread.sleep( 100 );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            // Exit
        }
    }

    private void shutdown()
    {
        if ( shutdownInitiated.compareAndSet( false, true ) )
        {
            log.info( "Shutting down..." );
            try
            {
                if ( getGraphDb() != null )
                {
                    getGraphDb().shutdown();
                    this.embeddedDb = null;
                }
            }
            catch ( Throwable t )
            {
                log.warning( "Error shutting down Neo: " + t );
            }
        }
    }

    public void execute( Map<String, String> arguments )
    {
        addShutdownHook();
        initialize( arguments );
        blockUntilShutdown();
    }

    public static void main( String[] args )
    {
        Map<String, String> arguments = new HashMap<String, String>();
        for ( int i = 0; i < args.length; i++ )
        {
            String arg = args[i];
            if ( arg.startsWith( "-" ) )
            {
                String key = arg.substring( 1 );
                String value = ++i < args.length ? args[i] : null;
                arguments.put( key, value );
            }
        }
        if ( !arguments.containsKey( "path" ) )
        {
            System.out.println( "Use -path <path> to control the neo4j store path" );
            return;
        }

        new StandaloneWithShell().execute( arguments );
    }
}
