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
package org.neo4j.server;

import java.io.File;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.BufferingConsoleLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.validation.DatabaseLocationMustBeSpecifiedRule;
import org.neo4j.server.configuration.validation.Validator;

import static java.lang.String.format;

public abstract class Bootstrapper
{
    public static final Integer OK = 0;
    public static final Integer WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final Integer GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;

    protected final LifeSupport life = new LifeSupport();
    protected NeoServer server;
	protected Configurator configurator;
    private Thread shutdownHook;
    protected Logging logging;
    private ConsoleLogger log;

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
            if ( candidate.isMoreDerivedThan( winner ) )
            {
                winner = candidate;
            }
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
            BufferingConsoleLogger consoleBuffer = new BufferingConsoleLogger();
        	configurator = createConfigurator( consoleBuffer );
        	logging = createLogging( configurator );
        	log = logging.getConsoleLog( getClass() );
        	consoleBuffer.replayInto( log );

        	life.start();

        	checkCompatibility();

            server = createNeoServer();
            server.start();

            addShutdownHook();

            return OK;
        }
        catch ( TransactionFailureException tfe )
        {
            log.error( format( "Failed to start Neo Server on port [%d], because ",
            		configurator.configuration().getInt( Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT ) )
                       + tfe + ". Another process may be using database location " + server.getDatabase()
                       .getLocation(), tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( IllegalArgumentException e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]",
            		configurator.configuration().getInt( Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT ) ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]",
            		configurator.configuration().getInt( Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT ) ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private Logging createLogging( Configurator configurator )
    {
        Config config = new Config( configurator.getDatabaseTuningProperties() );
        return life.add( DefaultLogging.createDefaultLogging( config ) );
    }

    private void checkCompatibility()
    {
        new JvmChecker( logging.getMessagesLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
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
            log.log( "Successfully shutdown Neo Server on port [%d], database [%s]",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT),
                    location );

            removeShutdownHook();

            life.shutdown();

            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%d], database [%s]. Reason [%s] ",
            		configurator.configuration().getInt(Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT), location, e.getMessage(), e );
            return 1;
        }
    }

    protected void removeShutdownHook()
    {
        if ( shutdownHook != null )
        {
            if ( !Runtime.getRuntime().removeShutdownHook( shutdownHook ) )
            {
                log.warn( "Unable to remove shutdown hook" );
            }
        }
    }

    public NeoServer getServer()
    {
        return server;
    }

    protected void addShutdownHook()
    {
        shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                log.log( "Neo4j Server shutdown initiated by request" );
                if ( server != null )
                {
                    server.stop();
                }
            }
        };
        Runtime.getRuntime()
                .addShutdownHook( shutdownHook );
    }

    protected Configurator createConfigurator( ConsoleLogger log )
    {
        File configFile = new File( System.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY,
                Configurator.DEFAULT_CONFIG_DIR ) );
        return new PropertyFileConfigurator( new Validator( new DatabaseLocationMustBeSpecifiedRule() ),
                configFile, log );
    }

    protected boolean isMoreDerivedThan( Bootstrapper other )
    {
        // Default implementation just checks if this is a subclass of other
        return other.getClass()
                .isAssignableFrom( getClass() );
    }
}
