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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;
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

    private Log log;
    private String serverPort;

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
        LogProvider userLogProvider = new ContextLessLogProviderDelegate( FormattedLogProvider.toOutputStream( System.out ) );

        JULBridge.resetJUL();
        Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( userLogProvider );
        JettyLogBridge.setLogProvider( userLogProvider );

        log = userLogProvider.getLog( getClass() );

        serverPort = "unknown port";
        try
        {
            configurator = createConfigurationBuilder( log );
            serverPort = String.valueOf( configurator.configuration().get( ServerSettings.webserver_port ) );
            dependencies = dependencies.userLogProvider( userLogProvider );

            life.start();

            checkCompatibility();

            server = createNeoServer( configurator, dependencies, userLogProvider );
            server.start();

            addShutdownHook();

            return OK;
        }
        catch ( TransactionFailureException tfe )
        {
            String locationMsg = (server == null) ? "" : " Another process may be using database location " +
                                                         server.getDatabase().getLocation();
            log.error( format( "Failed to start Neo Server on port [%s].", serverPort ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( IllegalArgumentException e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]", serverPort ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo Server on port [%s]", serverPort ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private void checkCompatibility()
    {
        new JvmChecker( log, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
    }

    protected abstract NeoServer createNeoServer( ConfigurationBuilder configurator, GraphDatabaseDependencies dependencies, LogProvider userLogProvider );

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
            log.info( "Successfully shutdown Neo Server on port [%s], database [%s]", serverPort, location );

            removeShutdownHook();

            life.shutdown();

            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%s], database [%s]. Reason [%s] ",
                    serverPort, location, e.getMessage(), e );
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
                log.info( "Neo4j Server shutdown initiated by request" );
                if ( server != null )
                {
                    server.stop();
                }
            }
        };
        Runtime.getRuntime()
                .addShutdownHook( shutdownHook );
    }

    protected Configurator createConfigurator( Log log )
    {
        return new ConfigurationBuilder.ConfigurationBuilderWrappingConfigurator( createConfigurationBuilder( log ) );
    }

    protected ConfigurationBuilder createConfigurationBuilder( Log log )
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

    private static class ContextLessLogProviderDelegate implements LogProvider
    {
        private LogProvider delegate;

        private ContextLessLogProviderDelegate( LogProvider delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public Log getLog( Class loggingClass )
        {
            return delegate.getLog( "" );
        }

        @Override
        public Log getLog( String context )
        {
            return delegate.getLog( "" );
        }
    }
}
