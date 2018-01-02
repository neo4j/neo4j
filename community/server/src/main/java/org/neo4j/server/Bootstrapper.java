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
package org.neo4j.server;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.BaseServerConfigLoader;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;
import org.neo4j.server.logging.Netty4LogBridge;
import static java.lang.String.format;
import static org.neo4j.server.web.ServerInternalSettings.SERVER_CONFIG_FILE;
import static org.neo4j.server.web.ServerInternalSettings.SERVER_CONFIG_FILE_KEY;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public abstract class Bootstrapper
{
    public static final int OK = 0;
    public static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    public static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;

    protected final LifeSupport life = new LifeSupport();
    protected NeoServer server;
    protected Config config;
    private Thread shutdownHook;
    protected GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();

    private Log log;
    private String serverPort;

    public static void main( String[] args )
    {
        throw new UnsupportedOperationException(
                "Invoking Bootstrapper#main() is no longer supported. If you see this error, please contact Neo4j " +
                "support." );
    }

    public int start( File configFile, Pair<String, String> ... configOverrides )
    {
        LogProvider userLogProvider = FormattedLogProvider.withoutRenderingContext().toOutputStream( System.out );

        JULBridge.resetJUL();
        Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( userLogProvider );
        JettyLogBridge.setLogProvider( userLogProvider );
        Netty4LogBridge.setLogProvider( userLogProvider );

        log = userLogProvider.getLog( getClass() );

        serverPort = "unknown port";
        try
        {
            config = createConfig( log, configFile, configOverrides );
            serverPort = String.valueOf( config.get( ServerSettings.webserver_port ) );
            dependencies = dependencies.userLogProvider( userLogProvider );

            life.start();

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
            String locationMsg = (server == null) ? "" : " Another process may be using database location " +
                                                         server.getDatabase().getLocation();
            log.error( format( "Failed to start Neo Server on port [%s].", serverPort ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
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

    protected abstract NeoServer createNeoServer( Config config, GraphDatabaseDependencies dependencies,
            LogProvider userLogProvider );

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

    /**
     * Create a new config instance for the DBMS. For legacy reasons, this method contains convoluted logic to load two additional config files - one
     * determined by a system property (neo4j-server.properties) and one determined by a config key (neo4j.properties).
     *
     * It will also override defaults set in neo4j embedded (remote shell default on/off, query log file name). Whether it's correct to do that here is
     * dubious, it makes it confusing in the documentation that the defaults do not match the behavior of the server.
     */
    protected Config createConfig( Log log, File file, Pair<String, String>[] configOverrides ) throws IOException
    {
        return new BaseServerConfigLoader().loadConfig( file, new File( System.getProperty( SERVER_CONFIG_FILE_KEY, SERVER_CONFIG_FILE ) ), log, configOverrides );
    }
}
