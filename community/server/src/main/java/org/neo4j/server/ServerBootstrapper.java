/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;

import static java.lang.String.format;

public abstract class ServerBootstrapper implements Bootstrapper
{
    public static final int OK = 0;
    public static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;

    private NeoServer server;
    private Thread shutdownHook;
    private GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
    private Log log;
    private String serverAddress = "unknown address";

    public static int start( Bootstrapper boot, String... argv )
    {
        ServerCommandLineArgs args = ServerCommandLineArgs.parse( argv );
        return boot.start( args.configFile(), args.configOverrides() );
    }

    @Override
    @SafeVarargs
    public final int start( Optional<File> configFile, Pair<String, String>... configOverrides )
    {
        LogProvider userLogProvider = setupLogging();
        dependencies = dependencies.userLogProvider( userLogProvider );
        log = userLogProvider.getLog( getClass() );

        try
        {
            Config config = createConfig( log, configFile, configOverrides );
            serverAddress = ServerSettings.httpConnector( config, ServerSettings.HttpConnector.Encryption.NONE )
                    .map( ( connector ) -> connector.address.toString() )
                    .orElse( serverAddress );

            checkCompatibility();

            server = createNeoServer( config, dependencies, userLogProvider );
            server.start();

            addShutdownHook();

            return OK;
        }
        catch ( ServerStartupException e )
        {
            e.describeTo( log );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
        catch ( TransactionFailureException tfe )
        {
            String locationMsg = (server == null) ? "" :
                    " Another process may be using database location " + server.getDatabase().getLocation();
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo4j on %s", serverAddress ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
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

            removeShutdownHook();

            return 0;
        }
        catch ( Exception e )
        {
            log.error( "Failed to cleanly shutdown Neo Server on port [%s], database [%s]. Reason [%s] ",
                    serverAddress, location, e.getMessage(), e );
            return 1;
        }
    }

    public boolean isRunning()
    {
        return server != null && server.getDatabase() != null && server.getDatabase().isRunning();
    }

    public NeoServer getServer()
    {
        return server;
    }

    protected abstract NeoServer createNeoServer( Config config, GraphDatabaseDependencies dependencies,
                                                  LogProvider userLogProvider );

    protected abstract Iterable<Class<?>> settingsClasses( Map<String, String> settings );

    private static LogProvider setupLogging()
    {
        LogProvider userLogProvider = FormattedLogProvider.withoutRenderingContext().toOutputStream( System.out );
        JULBridge.resetJUL();
        Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( userLogProvider );
        JettyLogBridge.setLogProvider( userLogProvider );
        return userLogProvider;
    }

    private Config createConfig( Log log, Optional<File> file, Pair<String, String>[] configOverrides ) throws IOException
    {
        return new ConfigLoader( this::settingsClasses ).loadConfig( file, log, configOverrides );
    }

    private void addShutdownHook()
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
        Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

    private void removeShutdownHook()
    {
        if ( shutdownHook != null )
        {
            if ( !Runtime.getRuntime().removeShutdownHook( shutdownHook ) )
            {
                log.warn( "Unable to remove shutdown hook" );
            }
        }
    }

    private void checkCompatibility()
    {
        new JvmChecker( log, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
    }
}
