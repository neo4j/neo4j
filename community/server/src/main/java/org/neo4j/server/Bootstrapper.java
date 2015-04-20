/*
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
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.BufferingConsoleLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.DefaultLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.ServerInternalSettings;

import static java.lang.String.format;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public abstract class Bootstrapper
{
    public static final Integer OK = 0;
    public static final Integer WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final Integer GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;

    protected final LifeSupport life = new LifeSupport();
    protected NeoServer server;
    protected ConfigurationBuilder configurator;
    private Thread shutdownHook;
    protected GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();

    // default logger to System.out so that we are always able to print errors even if logging has not been initialized
    private ConsoleLogger log = new ConsoleLogger( StringLogger.SYSTEM );

    public static void main( String[] args )
    {
        Bootstrapper bootstrapper = loadMostDerivedBootstrapper();
        Integer exit = bootstrapper.start();
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

    // TODO: method not used and WrapperListener interface does no exist. Check if it is safe to remove it
    public void controlEvent( int arg )
    {
        // Do nothing, required by the WrapperListener interface
    }

    // TODO: args are not used, check if it is safe to remove this method
    public Integer start( String[] args )
    {
        return start();
    }

    public Integer start()
    {
        try
        {
            dependencies = dependencies.monitors( new Monitors() );
            BufferingConsoleLogger consoleBuffer = new BufferingConsoleLogger();
            configurator = createConfigurationBuilder( consoleBuffer );
            dependencies = dependencies.logging( createLogging( configurator, dependencies.monitors() ) );
            log = dependencies.logging().getConsoleLog( getClass() );
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
            String locationMsg = (server == null) ? "" : " Another process may be using database location " +
                                                         server.getDatabase().getLocation();
            log.error( format( "Failed to start Neo Server on port [%s].", webServerPort() ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( IllegalArgumentException e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]", webServerPort() ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]", webServerPort() ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private Logging createLogging( ConfigurationBuilder configurator, Monitors monitors )
    {
        try
        {
            Config config = new Config( configurator.getDatabaseTuningProperties() );
            return life.add( DefaultLogging.createDefaultLogging( config, monitors ) );
        }
        catch ( RuntimeException e )
        {
            log.error( "Unable to initialize logging. Will fallback to System.out", e );
            return new SystemOutLogging();
        }
    }

    private void checkCompatibility()
    {
        new JvmChecker( dependencies.logging().getMessagesLog( JvmChecker.class ),
                new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
    }

    protected abstract NeoServer createNeoServer();

    // TODO: stopArg is not used, check if it is safe to remove this method
    public void stop( int stopArg )
    {
        stop();
    }

    public int stop()
    {
        String location = "unknown location";
        try
        {
            if ( server != null )
            {
                server.stop();
            }
            log.log( "Successfully shutdown Neo Server on port [%s], database [%s]", webServerPort(), location );

            removeShutdownHook();

            life.shutdown();

            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%s], database [%s]. Reason [%s] ",
                    webServerPort(), location, e.getMessage(), e );
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
        return new ConfigurationBuilder.ConfigurationBuilderWrappingConfigurator( createConfigurationBuilder( log ) );
    }

    protected ConfigurationBuilder createConfigurationBuilder( ConsoleLogger log )
    {
        File configFile = new File( System.getProperty(
                ServerInternalSettings.SERVER_CONFIG_FILE_KEY, Configurator.DEFAULT_CONFIG_DIR ) );
        return new PropertyFileConfigurator( configFile, log );
    }

    protected boolean isMoreDerivedThan( Bootstrapper other )
    {
        // Default implementation just checks if this is a subclass of other
        return other.getClass()
                .isAssignableFrom( getClass() );
    }

    private String webServerPort()
    {
        return (configurator == null)
               ? "unknown port"
               : String.valueOf( configurator.configuration().get( ServerSettings.webserver_port ) );
    }
}
