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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.info.JvmChecker;
import org.neo4j.kernel.info.JvmMetadataRepository;
import org.neo4j.kernel.impl.scheduler.BufferingExecutor;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.logging.RotatingFileOutputStreamSupplier;
import org.neo4j.scheduler.Group;
import org.neo4j.server.logging.JULBridge;
import org.neo4j.server.logging.JettyLogBridge;

import static java.lang.String.format;
import static org.neo4j.commandline.Util.neo4jVersion;
import static org.neo4j.io.file.Files.createOrOpenAsOutputStream;

public abstract class ServerBootstrapper implements Bootstrapper
{
    public static final int OK = 0;
    private static final int WEB_SERVER_STARTUP_ERROR_CODE = 1;
    private static final int GRAPH_DATABASE_STARTUP_ERROR_CODE = 2;
    private static final String SIGTERM = "TERM";
    private static final String SIGINT = "INT";

    private volatile NeoServer server;
    private volatile OutputStream userLogFileStream;
    private Thread shutdownHook;
    private GraphDatabaseDependencies dependencies = GraphDatabaseDependencies.newDependencies();
    // in case we have errors loading/validating the configuration log to stdout
    private Log log = FormattedLogProvider.toOutputStream( System.out ).getLog( getClass() );
    private String serverAddress = "unknown address";

    public static int start( Bootstrapper boot, String... argv )
    {
        ServerCommandLineArgs args = ServerCommandLineArgs.parse( argv );

        if ( args.version() )
        {
            System.out.println( "neo4j " + neo4jVersion() );
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
        try
        {
            // Create config file from arguments
            Config config = Config.builder()
                    .withFile( configFile )
                    .withSettings( configOverrides )
                    .withHome(homeDir)
                    .withValidators( configurationValidators() )
                    .withNoThrowOnFileLoadFailure() // TODO 4.0: Remove this, and require a neo4j.conf file to be present?
                    .withServerDefaults().build();

            LogProvider userLogProvider = setupLogging( config );
            dependencies = dependencies.userLogProvider( userLogProvider );
            log = userLogProvider.getLog( getClass() );
            config.setLogger( log );

            serverAddress =  config.httpConnectors().stream()
                    .filter( c -> Encryption.NONE.equals( c.encryptionLevel() ) )
                    .findFirst()
                    .map( connector -> config.get( connector.listen_address ).toString() )
                    .orElse( serverAddress );

            checkCompatibility();

            server = createNeoServer( config, dependencies );
            server.start();

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
            log.error( format( "Failed to start Neo4j on %s.", serverAddress ), e );
            return WEB_SERVER_STARTUP_ERROR_CODE;
        }
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
        return server != null && server.getDatabase() != null && server.getDatabase().isRunning();
    }

    public NeoServer getServer()
    {
        return server;
    }

    private NeoServer createNeoServer( Config config, GraphDatabaseDependencies dependencies )
    {
        GraphFactory graphFactory = createGraphFactory( config );

        boolean httpAndHttpsDisabled = config.enabledHttpConnectors().isEmpty();
        if ( httpAndHttpsDisabled )
        {
            return new DisabledNeoServer( graphFactory, dependencies, config );
        }
        return createNeoServer( graphFactory, config, dependencies );
    }

    protected abstract GraphFactory createGraphFactory( Config config );

    /**
     * Create a new server component. This method is invoked only when at least one HTTP connector is enabled.
     */
    protected abstract NeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseDependencies dependencies );

    protected Collection<ConfigurationValidator> configurationValidators()
    {
        return Collections.emptyList();
    }

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
        if ( server != null )
        {
            server.stop();
        }
        if ( userLogFileStream != null )
        {
            closeUserLogFileStream();
        }
    }

    private void closeUserLogFileStream()
    {
        try ( OutputStream stream = userLogFileStream )
        {
            stream.flush();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
        dependencies.withDeferredExecutor( deferredExecutor, Group.LOG_ROTATION  );

        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File destination = config.get( GraphDatabaseSettings.store_user_log_path );
        Long rotationThreshold = config.get( GraphDatabaseSettings.store_user_log_rotation_threshold );
        try
        {
            if ( rotationThreshold == 0L )
            {
                userLogFileStream = createOrOpenAsOutputStream( fs, destination, true );
                return builder.toOutputStream( userLogFileStream );
            }
            return builder.toOutputStream(
                    new RotatingFileOutputStreamSupplier(
                            fs,
                            destination,
                            rotationThreshold,
                            config.get( GraphDatabaseSettings.store_user_log_rotation_delay ).toMillis(),
                            config.get( GraphDatabaseSettings.store_user_log_max_archives ),
                            deferredExecutor )
            );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void checkCompatibility()
    {
        new JvmChecker( log, new JvmMetadataRepository() ).checkJvmCompatibilityAndIssueWarning();
    }
}
