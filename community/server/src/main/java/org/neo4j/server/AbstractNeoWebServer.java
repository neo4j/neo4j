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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.bind.ComponentsBinder;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.http.cypher.HttpTransactionManager;
import org.neo4j.server.http.cypher.TransactionRegistry;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.InputFormatProvider;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.OutputFormatProvider;
import org.neo4j.server.rest.repr.RepresentationFormatRepository;
import org.neo4j.server.web.RotatingRequestLog;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.Clocks;

import static java.lang.String.format;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.server.configuration.ServerSettings.http_log_path;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_keep_number;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_size;

public abstract class AbstractNeoWebServer extends LifecycleAdapter implements NeoWebServer
{
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     * <p>
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    static final String NEO4J_IS_STARTING_MESSAGE = "======== Neo4j " + Version.getNeo4jVersion() + " ========";

    protected final LogProvider userLogProvider;
    private final Log log;
    private final DbmsInfo dbmsInfo;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final List<Pattern> authWhitelist;
    private final DatabaseManagementService databaseManagementService;
    private final Dependencies globalDependencies;
    private final Config config;
    private final LifeSupport life = new LifeSupport();
    private final boolean httpEnabled;
    private final boolean httpsEnabled;
    private SocketAddress httpListenAddress;
    private SocketAddress httpsListenAddress;
    private SocketAddress httpAdvertisedAddress;
    private SocketAddress httpsAdvertisedAddress;

    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    private Supplier<SslPolicyLoader> sslPolicyFactorySupplier;
    private HttpTransactionManager httpTransactionManager;
    private CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;

    protected ConnectorPortRegister connectorPortRegister;
    private RotatingRequestLog requestLog;

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    public AbstractNeoWebServer( DatabaseManagementService databaseManagementService, Dependencies globalDependencies, Config config,
            LogProvider userLogProvider, DbmsInfo dbmsInfo )
    {
        this.databaseManagementService = databaseManagementService;
        this.globalDependencies = globalDependencies;
        this.config = config;
        this.userLogProvider = userLogProvider;
        this.log = userLogProvider.getLog( getClass() );
        this.dbmsInfo = dbmsInfo;
        log.info( NEO4J_IS_STARTING_MESSAGE );

        verifyConnectorsConfiguration( config );

        httpEnabled = config.get( HttpConnector.enabled );
        if ( httpEnabled )
        {
            httpListenAddress = config.get( HttpConnector.listen_address );
            httpAdvertisedAddress = config.get( HttpConnector.advertised_address );
        }

        httpsEnabled = config.get( HttpsConnector.enabled );
        if ( httpsEnabled )
        {
            httpsListenAddress = config.get( HttpsConnector.listen_address );
            httpsAdvertisedAddress = config.get( HttpsConnector.advertised_address );
        }

        this.authWhitelist = parseAuthWhitelist( config );
        authManagerSupplier = globalDependencies.provideDependency( AuthManager.class );
        sslPolicyFactorySupplier = globalDependencies.provideDependency( SslPolicyLoader.class );
        connectorPortRegister = globalDependencies.resolveDependency( ConnectorPortRegister.class );
        httpTransactionManager = createHttpTransactionManager();
        globalAvailabilityGuard = globalDependencies.resolveDependency( CompositeDatabaseAvailabilityGuard.class );

        life.add( new ServerComponentsLifecycleAdapter() );
    }

    protected Dependencies getGlobalDependencies()
    {
        return globalDependencies;
    }

    @Override
    public DbmsInfo getDbmsInfo()
    {
        return dbmsInfo;
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start() throws ServerStartupException
    {
        try
        {
            life.start();
        }
        catch ( Throwable t )
        {
            // If the database has been started, attempt to cleanly shut it down to avoid unclean shutdowns.
            life.shutdown();
            var rootCause = ExceptionUtils.getRootCause( t );
            throw new ServerStartupException( format( "Starting Neo4j failed: %s", rootCause.getMessage() ), rootCause );
        }
    }

    private HttpTransactionManager createHttpTransactionManager()
    {
        JobScheduler jobScheduler = globalDependencies.resolveDependency( JobScheduler.class );
        Clock clock = Clocks.systemClock();
        Duration transactionTimeout = getTransactionTimeout();
        return new HttpTransactionManager( databaseManagementService, jobScheduler, clock, transactionTimeout, userLogProvider );
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private Duration getTransactionTimeout()
    {
        final long timeout = config.get( ServerSettings.transaction_idle_timeout ).toMillis();
        return Duration.ofMillis( Math.max( timeout, MINIMUM_TIMEOUT + ROUNDING_SECOND ) );
    }

    /**
     * Use this method to register server modules from subclasses
     */
    private void registerModule( ServerModule module )
    {
        serverModules.add( module );
    }

    private void startModules()
    {
        for ( ServerModule module : serverModules )
        {
            module.start();
        }
    }

    private void stopModules()
    {
        final List<Exception> errors = new ArrayList<>();
        for ( final ServerModule module : serverModules )
        {
            try
            {
                module.stop();
            }
            catch ( Exception e )
            {
                errors.add( e );
            }
        }
        if ( !errors.isEmpty() )
        {
            final RuntimeException e = new RuntimeException();
            errors.forEach( e::addSuppressed );
            throw e;
        }
    }

    private void clearModules()
    {
        serverModules.clear();
    }

    @Override
    public Config getConfig()
    {
        return config;
    }

    protected void configureWebServer()
    {
        webServer.setHttpAddress( httpListenAddress );
        webServer.setHttpsAddress( httpsListenAddress );
        webServer.setMaxThreads( config.get( ServerSettings.webserver_max_threads ) );
        webServer.setWadlEnabled( config.get( ServerSettings.wadl_enabled ) );
        webServer.setComponentsBinder( createComponentsBinder() );

        if ( httpsEnabled ) // only load sslPolicy when encryption is enabled
        {
            SslPolicyLoader sslPolicyLoader = sslPolicyFactorySupplier.get();
            if ( sslPolicyLoader.hasPolicyForSource( HTTPS ) )
            {
                webServer.setSslPolicy( sslPolicyLoader.getPolicy( HTTPS ) );
            }
        }
    }

    protected void startWebServer() throws Exception
    {
        try
        {
            setUpHttpLogging();
            webServer.start();
            registerHttpAddressAfterStartup();
            registerHttpsAddressAfterStartup();
            log.info( "Remote interface available at %s", getBaseUri() );
        }
        catch ( Exception e )
        {
            SocketAddress address = httpListenAddress != null ? httpListenAddress : httpsListenAddress;
            log.error( "Failed to start Neo4j on %s: %s", address, e.getMessage() );
            throw e;
        }
    }

    private void registerHttpAddressAfterStartup()
    {
        if ( httpEnabled )
        {
            InetSocketAddress localHttpAddress = webServer.getLocalHttpAddress();
            connectorPortRegister.register( HttpConnector.NAME, localHttpAddress );
            if ( httpAdvertisedAddress.getPort() == 0 )
            {
                httpAdvertisedAddress = new SocketAddress( localHttpAddress.getHostString(), localHttpAddress.getPort() );
            }
        }
    }

    private void registerHttpsAddressAfterStartup()
    {
        if ( httpsEnabled )
        {
            InetSocketAddress localHttpsAddress = webServer.getLocalHttpsAddress();
            connectorPortRegister.register( HttpsConnector.NAME, localHttpsAddress );
            if ( httpsAdvertisedAddress.getPort() == 0 )
            {
                httpsAdvertisedAddress = new SocketAddress( localHttpsAddress.getHostString(), localHttpsAddress.getPort() );
            }
        }
    }

    private void setUpHttpLogging() throws IOException
    {
        if ( !getConfig().get( http_logging_enabled ) )
        {
            return;
        }

        requestLog = new RotatingRequestLog(
                globalDependencies.resolveDependency( FileSystemAbstraction.class ),
                config.get( db_timezone ),
                config.get( http_log_path ).toString(),
                config.get( http_logging_rotation_size ),
                config.get( http_logging_rotation_keep_number ) );
        webServer.setRequestLog( requestLog );
    }

    protected List<Pattern> getUriWhitelist()
    {
        return authWhitelist;
    }

    @Override
    public void stop()
    {
        shutdownGlobalAvailabilityGuard();
        life.stop();
    }

    private void shutdownGlobalAvailabilityGuard()
    {
        try
        {
            // Although the globalGuard availability guard is shutdown as part of LifeSupport#stop(), we never hit that if we're
            // blocking in LifeSupport#start() and the blocked starting components may be using this guard as a bail out signal
            if ( globalAvailabilityGuard != null )
            {
                globalAvailabilityGuard.stop();
            }
        }
        catch ( Throwable t )
        {
            // Not much we can do other than log - we're trying to shutdown anyway
            log.error( "Failed to set the global availability guard to shutdown in the process of stopping the Neo4j server", t );
        }
    }

    private void stopWebServer() throws Exception
    {
        if ( webServer != null )
        {
            webServer.stop();
        }
        if ( requestLog != null )
        {
            requestLog.stop();
        }
    }

    @Override
    public TransactionRegistry getTransactionRegistry()
    {
        return httpTransactionManager.getTransactionHandleRegistry();
    }

    @Override
    public URI getBaseUri()
    {
        return httpAdvertisedAddress != null
               ? uriBuilder.buildURI( httpAdvertisedAddress, false )
               : uriBuilder.buildURI( httpsAdvertisedAddress, true );
    }

    @Override
    public Optional<URI> httpsUri()
    {
        return Optional.ofNullable( httpsAdvertisedAddress )
                .map( address -> uriBuilder.buildURI( address, true ) );
    }

    public WebServer getWebServer()
    {
        return webServer;
    }

    private ComponentsBinder createComponentsBinder()
    {
        ComponentsBinder binder = new ComponentsBinder();

        var databaseStateService = getGlobalDependencies().resolveDependency( DatabaseStateService.class );
        binder.addSingletonBinding( databaseManagementService, DatabaseManagementService.class );
        binder.addSingletonBinding( databaseStateService, DatabaseStateService.class );
        binder.addSingletonBinding( this, NeoWebServer.class );
        binder.addSingletonBinding( getConfig(), Config.class );
        binder.addSingletonBinding( getWebServer(), WebServer.class );
        binder.addSingletonBinding( new RepresentationFormatRepository(), RepresentationFormatRepository.class );
        binder.addLazyBinding( InputFormatProvider.class, InputFormat.class );
        binder.addLazyBinding( OutputFormatProvider.class, OutputFormat.class );
        binder.addSingletonBinding( httpTransactionManager, HttpTransactionManager.class );
        binder.addLazyBinding( authManagerSupplier, AuthManager.class );
        binder.addSingletonBinding( userLogProvider, LogProvider.class );
        binder.addSingletonBinding( userLogProvider.getLog( NeoWebServer.class ), Log.class );

        return binder;
    }

    private static void verifyConnectorsConfiguration( Config config )
    {
        boolean httpAndHttpsDisabled = !config.get( HttpConnector.enabled ) && !config.get( HttpsConnector.enabled );
        if ( httpAndHttpsDisabled )
        {
            throw new IllegalArgumentException( "Either HTTP or HTTPS connector must be configured to run the server" );
        }
    }

    private static List<Pattern> parseAuthWhitelist( Config config )
    {
        return config.get( ServerSettings.http_auth_allowlist )
                .stream()
                .map( Pattern::compile )
                .collect( toUnmodifiableList() );
    }

    private class ServerComponentsLifecycleAdapter extends LifecycleAdapter
    {
        @Override
        public void init()
        {
            webServer = createWebServer();

            for ( ServerModule moduleClass : createServerModules() )
            {
                registerModule( moduleClass );
            }
        }

        @Override
        public void start() throws Exception
        {
            LogService logService = globalDependencies.resolveDependency( LogService.class );
            Log serverLog = logService.getInternalLog( ServerComponentsLifecycleAdapter.class );
            serverLog.info( "Starting web server" );
            configureWebServer();

            startModules();

            startWebServer();

            serverLog.info( "Web server started." );
        }

        @Override
        public void stop() throws Exception
        {
            stopWebServer();
            stopModules();
            clearModules();
        }
    }
}
