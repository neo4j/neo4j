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
package org.neo4j.server;

import java.io.File;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;
import org.neo4j.server.logging.JulAdapter;
import org.neo4j.server.logging.Logger;

public abstract class Bootstrapper
{
    public static final Integer OK = 0;
    public static final Integer WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final Integer GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;

    private static Logger log = Logger.getLogger( CommunityBootstrapper.class );

    protected NeoServer server;
	private Configurator configurator;

    public static void main( String[] args )
    {
        Bootstrapper bootstrapper = loadMostDerivedBootstrapper();
        Integer exit = bootstrapper.start( args );
        if ( exit != 0 )
        {
            System.exit( exit );
        }
    }

    public static Bootstrapper loadMostDerivedBootstrapper()
    {
        Bootstrapper winner = new CommunityBootstrapper();
        for ( Bootstrapper candidate : Service.load( Bootstrapper.class ) )
        {
            if ( candidate.isMoreDerivedThan( winner ) ) winner = candidate;
        }
        return winner;
    }

    public void controlEvent( int arg )
    {
        // Do nothing, required by the WrapperListener interface
    }

    public Integer start()
    {
        return start( new String[0] );
    }

    // TODO: This does not use args, check if it is safe to remove them
    public Integer start( String[] args )
    {
        try
        {
            checkCompatibility();

        	configurator = createConfigurator();

            server = createNeoServer();
            server.start();

            addShutdownHook();

            return OK;
        }
        catch ( TransactionFailureException tfe )
        {
            log.error(tfe);
            log.error( String.format( "Failed to start Neo Server on port [%d], because ",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT) )
                       + tfe + ". Another process may be using database location " + server.getDatabase()
                               .getLocation() );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error(e);
            log.error( "Failed to start Neo Server on port [%s]",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT) );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private void checkCompatibility()
    {
        new JvmChecker( new JulAdapter( log ), new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
    }

    protected abstract NeoServer createNeoServer();

	public void stop()
    {
        stop( 0 );
    }

    public int stop( int stopArg )
    {
        String location = "unknown location";
        try
        {
            if ( server != null )
            {
                server.stop();
            }
            log.info( "Successfully shutdown Neo Server on port [%d], database [%s]",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT),
                    location );
            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%d], database [%s]. Reason [%s] ",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT), location, e.getMessage() );
            return 1;
        }
    }

    public NeoServer getServer()
    {
        return server;
    }

    protected void addShutdownHook()
    {
        Runtime.getRuntime()
                .addShutdownHook( new Thread()
                {
                    @Override
                    public void run()
                    {
                        log.info( "Neo4j Server shutdown initiated by kill signal" );
                        if ( server != null )
                        {
                            server.stop();
                        }
                    }
                } );
    }

    protected Configurator createConfigurator()
    {
        File configFile = new File( System.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY,
                Configurator.DEFAULT_CONFIG_DIR ) );
        return new PropertyFileConfigurator( new Validator( new DatabaseLocationMustBeSpecifiedRule() ), configFile );
    }

    protected boolean isMoreDerivedThan( Bootstrapper other )
    {
        // Default implementation just checks if this is a subclass of other
        return other.getClass()
                .isAssignableFrom( getClass() );
    }
}
