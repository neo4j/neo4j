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
package org.neo4j.server;

import sun.misc.Signal;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GroupSettingValidator;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.scheduler.BufferingExecutor;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.scheduler.Group;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.io.fs.FileSystemUtils.createOrOpenAsOutputStream;

public abstract class NeoBootstrapper implements Bootstrapper
{
    public static final int OK = 0;
    private static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    private static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    private static final String SIGTERM = "TERM";
    private static final String SIGINT = "INT";

    private volatile DatabaseManagementService databaseManagementService;
    private volatile Closeable userLogFileStream;
    private Thread shutdownHook;
    private GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
    // in case we have errors loading/validating the configuration log to stdout
    private Log log = FormattedLogProvider.toOutputStream( System.out ).getLog( getClass() );
    private String serverAddress = "unknown address";

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

        return boot.start( args.homeDir(), args.configFile(), args.configOverrides() );
    }

    @Override
    public final int start( File homeDir, Optional<File> configFile, Map<String, String> configOverrides )
    {
        addShutdownHook();
        installSignalHandlers();
        Config config = Config.newBuilder()
                .setDefaults( GraphDatabaseSettings.SERVER_DEFAULTS )
                .fromFileNoThrow( configFile.orElse( null ) )
                .setRaw( configOverrides )
                .set( GraphDatabaseSettings.neo4j_home, homeDir.toPath().toAbsolutePath() )
                .addValidators( configurationValidators() )
                .build();
        try
        {
            LogProvider userLogProvider = setupLogging( config );
            dependencies = dependencies.userLogProvider( userLogProvider );
            log = userLogProvider.getLog( getClass() );
            config.setLogger( log );

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
                                 " Another process may be using databases at location: " + config.get( GraphDatabaseSettings.databases_root_path );
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ) + locationMsg, tfe );
            return GRAPH_DATABASE_STARTUP_ERROR_CODE;
        }
        catch ( Exception e )
        {
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
    }

    protected List<Class<? extends GroupSettingValidator>> configurationValidators()
    {
        return emptyList();
    }

    @Override
    public int stop()
    {
        String location = "unknown location";
        try
        {
            doShutdown();

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

    private LogProvider setupLogging( Config config )
    {
        FormattedLogProvider.Builder builder = FormattedLogProvider
                .withoutRenderingContext()
                .withZoneId( config.get( GraphDatabaseSettings.db_timezone ).getZoneId() )
                .withDefaultLogLevel( config.get( GraphDatabaseSettings.store_internal_log_level ) );

        LogProvider userLogProvider = config.get( GraphDatabaseSettings.store_user_log_to_stdout ) ? builder.toOutputStream( System.out )
                                                                                                   : createFileSystemUserLogProvider( config, builder );

        JULBridge.resetJUL();
        Logger.getLogger( "" ).setLevel( Level.WARNING );
        JULBridge.forwardTo( userLogProvider );
        JettyLogBridge.setLogProvider( userLogProvider );
        return userLogProvider;
    }

    // Exit gracefully if possible
    private void installSignalHandlers()
    {
        installSignalHandler( SIGTERM, false ); // SIGTERM is invoked when system service is stopped
        installSignalHandler( SIGINT, true ); // SIGINT is invoked when user hits ctrl-c  when running `neo4j console`
    }

    private void installSignalHandler( String sig, boolean tolerateErrors )
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
        if ( databaseManagementService != null )
        {
            log.info( "Stopping..." );
            databaseManagementService.shutdown();
            log.info( "Stopped." );
        }
        if ( userLogFileStream != null )
        {
            closeUserLogFileStream();
        }
    }

    private void closeUserLogFileStream()
    {
        IOUtils.closeAllUnchecked( userLogFileStream );
    }

    private void addShutdownHook()
    {
        shutdownHook = new Thread( () -> {
            log.info( "Neo4j Server shutdown initiated by request" );
            doShutdown();
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

    private LogProvider createFileSystemUserLogProvider( Config config, FormattedLogProvider.Builder builder )
    {
        BufferingExecutor deferredExecutor = new BufferingExecutor();
        dependencies = dependencies.withDeferredExecutor( deferredExecutor, Group.LOG_ROTATION );

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File destination = config.get( GraphDatabaseSettings.store_user_log_path ).toFile();
        Long rotationThreshold = config.get( GraphDatabaseSettings.store_user_log_rotation_threshold );
        try
        {
            if ( rotationThreshold == 0L )
            {
                OutputStream userLog = createOrOpenAsOutputStream( fs, destination, true );
                // Assign it to the server instance so that it gets closed when the server closes
                this.userLogFileStream = userLog;
                return builder.toOutputStream( userLog );
            }
            RotatingFileOutputStreamSupplier rotatingUserLogSupplier = new RotatingFileOutputStreamSupplier( fs, destination, rotationThreshold,
                    config.get( GraphDatabaseSettings.store_user_log_rotation_delay ).toMillis(),
                    config.get( GraphDatabaseSettings.store_user_log_max_archives ), deferredExecutor );
            // Assign it to the server instance so that it gets closed when the server closes
            this.userLogFileStream = rotatingUserLogSupplier;
            return builder.toOutputStream( rotatingUserLogSupplier );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
