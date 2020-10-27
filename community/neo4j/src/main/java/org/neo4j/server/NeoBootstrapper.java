/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server;

import sun.misc.Signal;

import java.io.Closeable;
import java.lang.management.MemoryUsage;
import java.nio.file.Path;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.configuration.BufferingLog;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ExternalSettings;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;
import org.neo4j.logging.log4j.Neo4jLoggerContext;
import org.neo4j.memory.MachineMemory;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;

public abstract class NeoBootstrapper implements Bootstrapper
{
    public static final int OK = 0;
    private static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    private static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    private static final int INVALID_CONFIGURATION_ERROR_CODE = 3;
    private static final String SIGTERM = "TERM";
    private static final String SIGINT = "INT";

    private volatile DatabaseManagementService databaseManagementService;
    private volatile Closeable userLogFileStream;
    private Thread shutdownHook;
    private GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
    private final BufferingLog startupLog = new BufferingLog();
    private volatile Log log = startupLog;
    private String serverAddress = "unknown address";
    private MachineMemory machineMemory = MachineMemory.DEFAULT;

    public static int start( Bootstrapper boot, String... argv )
    {
        CommandLineArgs args = CommandLineArgs.parse( argv );

        if ( args.version() )
        {
            System.out.println( "neo4j " + Version.getNeo4jVersion() );
            return 0;
        }

        if ( args.homeDir() == null )
        {
            throw new ServerStartupException( "Argument --home-dir is required and was not provided." );
        }

        return boot.start( args.homeDir(), args.configFile(), args.configOverrides(), args.expandCommands() );
    }

    @VisibleForTesting
    public final int start( Path homeDir, Map<String, String> configOverrides )
    {
        return start( homeDir, null, configOverrides, false );
    }

    @Override
    public final int start( Path homeDir, Path configFile, Map<String,String> configOverrides, boolean expandCommands )
    {
        addShutdownHook();
        installSignalHandlers();
        Config config = Config.newBuilder()
                .commandExpansion( expandCommands )
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .fromFileNoThrow( configFile )
                .setRaw( configOverrides )
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toAbsolutePath() )
                .build();
        Log4jLogProvider userLogProvider = setupLogging( config );
        userLogFileStream = userLogProvider;

        dependencies = dependencies.userLogProvider( userLogProvider );

        log = userLogProvider.getLog( getClass() );
        // Log any messages written before logging was configured.
        startupLog.replayInto( log );

        config.setLogger( log );

        if ( requestedMemoryExceedsAvailable( config ) )
        {
            log.error( format( "Invalid memory configuration - exceeds physical memory. Check the configured values for %s and %s",
                    GraphDatabaseSettings.pagecache_memory.name(), ExternalSettings.max_heap_size.name() ) );
            return INVALID_CONFIGURATION_ERROR_CODE;
        }

        try
        {
            serverAddress = HttpConnector.listen_address.toString();

            log.info( "Starting..." );
            databaseManagementService = createNeo( config, dependencies );
            log.info( "Started." );

            return OK;
        }
        catch ( ServerStartupException e )
        {
            e.describeTo( log );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
        catch ( TransactionFailureException tfe )
        {
            String locationMsg = (databaseManagementService == null) ? "" :
                                 " Another process may be using databases at location: " + config.get( GraphDatabaseInternalSettings.databases_root_path );
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    private boolean requestedMemoryExceedsAvailable( Config config )
    {
        String pageCacheMemory = config.get( GraphDatabaseSettings.pagecache_memory );
        long pageCacheSize =
                pageCacheMemory == null ? ConfiguringPageCacheFactory.defaultHeuristicPageCacheMemory( machineMemory ) : ByteUnit.parse( pageCacheMemory );
        MemoryUsage heapMemoryUsage = machineMemory.getHeapMemoryUsage();
        long totalPhysicalMemory = machineMemory.getTotalPhysicalMemory();

        return totalPhysicalMemory != OsBeanUtil.VALUE_UNAVAILABLE  && pageCacheSize + heapMemoryUsage.getMax() > totalPhysicalMemory;
    }

    @Override
    public int stop()
    {
        String location = "unknown location";
        try
        {
            doShutdown();

            removeShutdownHook();

            closeUserLogFileStream();
            return 0;
        }
        catch ( Exception e )
        {
            switchToErrorLoggingIfLoggingNotConfigured();
            log.error( "Failed to cleanly shutdown Neo Server on port [%s], database [%s]. Reason [%s] ",
                    serverAddress, location, e.getMessage(), e );
            closeUserLogFileStream();
            return 1;
        }
    }

    public boolean isRunning()
    {
        return databaseManagementService != null;
    }

    public DatabaseManagementService getDatabaseManagementService()
    {
        return databaseManagementService;
    }

    public Log getLog()
    {
        return log;
    }

    protected abstract DatabaseManagementService createNeo( Config config, GraphDatabaseDependencies dependencies );

    private Log4jLogProvider setupLogging( Config config )
    {

        LogConfig.Builder builder =
                LogConfig.createBuilder( new DefaultFileSystemAbstraction(), config.get( GraphDatabaseSettings.store_user_log_path ),
                        config.get( GraphDatabaseSettings.store_internal_log_level ) )
                         .withTimezone( config.get( GraphDatabaseSettings.db_timezone ) )
                         .withFormat( config.get( GraphDatabaseInternalSettings.log_format ) )
                         .withCategory( false )
                         .withRotation( config.get( GraphDatabaseSettings.store_user_log_rotation_threshold ),
                                 config.get( GraphDatabaseSettings.store_user_log_max_archives ) );

        if ( config.get( GraphDatabaseSettings.store_user_log_to_stdout ) )
        {
            builder.logToSystemOut();
        }

        Neo4jLoggerContext ctx = builder.build();
        Log4jLogProvider userLogProvider = new Log4jLogProvider( ctx );

        JULBridge.resetJUL();
        Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( userLogProvider );
        JettyLogBridge.setLogProvider( userLogProvider );
        return userLogProvider;
    }

    // Exit gracefully if possible
    private static void installSignalHandlers()
    {
        installSignalHandler( SIGTERM, false ); // SIGTERM is invoked when system service is stopped
        installSignalHandler( SIGINT, true ); // SIGINT is invoked when user hits ctrl-c  when running `neo4j console`
    }

    private static void installSignalHandler( String sig, boolean tolerateErrors )
    {
        try
        {
            // System.exit() will trigger the shutdown hook
            Signal.handle( new Signal( sig ), signal -> System.exit( 0 ) );
        }
        catch ( Throwable e )
        {
            if ( !tolerateErrors )
            {
                throw e;
            }
            // Errors occur on IBM JDK with IllegalArgumentException: Signal already used by VM: INT
            // I can't find anywhere where we send a SIGINT to neo4j process so I don't think this is that important
        }
    }

    private void doShutdown()
    {
        switchToErrorLoggingIfLoggingNotConfigured();
        if ( databaseManagementService != null )
        {
            log.info( "Stopping..." );
            databaseManagementService.shutdown();
        }
        log.info( "Stopped." );
    }

    private void closeUserLogFileStream()
    {
        if ( userLogFileStream != null )
        {
            IOUtils.closeAllUnchecked( userLogFileStream );
        }
    }

    private void addShutdownHook()
    {
        shutdownHook = new Thread( () -> {
            log.info( "Neo4j Server shutdown initiated by request" );
            doShutdown();
            closeUserLogFileStream();
        } );
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

    /**
     * If we ran into an error before logging was properly setup we log what we have buffered and any following messages directly to System.out.
     */
    private void switchToErrorLoggingIfLoggingNotConfigured()
    {
        // Logging isn't configured yet
        if ( userLogFileStream == null )
        {
            Log4jLogProvider outProvider = new Log4jLogProvider( System.out );
            userLogFileStream = outProvider;
            log = outProvider.getLog( getClass() );
            startupLog.replayInto( log );
        }
    }

    @VisibleForTesting
    void setMachineMemory( MachineMemory machineMemory )
    {
        this.machineMemory = machineMemory;
    }
}
