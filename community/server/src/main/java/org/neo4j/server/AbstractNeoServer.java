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

import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.RunCarefully;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.CypherExecutorProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.ConfigAdapter;
import org.neo4j.server.plugins.PluginInvocatorProvider;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.rest.repr.InputFormatProvider;
import org.neo4j.server.rest.repr.OutputFormatProvider;
import org.neo4j.server.rest.repr.RepresentationFormatRepository;
import org.neo4j.server.rest.transactional.TransactionFacade;
import org.neo4j.server.rest.transactional.TransactionFilter;
import org.neo4j.server.rest.transactional.TransactionHandleRegistry;
import org.neo4j.server.rest.transactional.TransactionRegistry;
import org.neo4j.server.rest.transactional.TransitionalPeriodTransactionMessContainer;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.server.web.AsyncRequestLog;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.web.WebServerProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.db_timezone;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.server.configuration.ServerSettings.http_log_path;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_keep_number;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_size;
import static org.neo4j.server.database.InjectableProvider.providerForSingleton;
import static org.neo4j.server.database.InjectableProvider.providerFromSupplier;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

public abstract class AbstractNeoServer implements NeoServer
{
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     * <p>
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    private static final Pattern[] DEFAULT_URI_WHITELIST = new Pattern[]{
            Pattern.compile( "/browser.*" ),
            Pattern.compile( "/" )
    };
    public static final String NEO4J_IS_STARTING_MESSAGE = "======== Neo4j " + Version.getNeo4jVersion() + " ========";

    protected final LogProvider userLogProvider;
    private final Log log;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final Config config;
    private final LifeSupport life = new LifeSupport();
    private final ListenSocketAddress httpListenAddress;
    private final ListenSocketAddress httpsListenAddress;
    private AdvertisedSocketAddress httpAdvertisedAddress;
    private AdvertisedSocketAddress httpsAdvertisedAddress;

    protected final Database database;
    private DependencyResolver dependencyResolver;
    protected CypherExecutor cypherExecutor;
    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    protected Supplier<UserManagerSupplier> userManagerSupplier;
    protected Supplier<SslPolicyLoader> sslPolicyFactorySupplier;
    private DatabaseActions databaseActions;
    private TransactionFacade transactionFacade;

    private TransactionHandleRegistry transactionRegistry;
    private ConnectorPortRegister connectorPortRegister;
    private HttpConnector httpConnector;
    private HttpConnector httpsConnector;
    private AsyncRequestLog requestLog;
    private final Supplier<AvailabilityGuard> availabilityGuardSupplier;

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    public AbstractNeoServer( Config config, GraphFactory graphFactory, Dependencies dependencies )
    {
        this.config = config;
        this.userLogProvider = dependencies.userLogProvider();
        this.log = userLogProvider.getLog( getClass() );
        log.info( NEO4J_IS_STARTING_MESSAGE );

        verifyConnectorsConfiguration( config );

        httpConnector = findConnector( config, Encryption.NONE );
        httpListenAddress = listenAddressFor( config, httpConnector );
        httpAdvertisedAddress = advertisedAddressFor( config, httpConnector );

        httpsConnector = findConnector( config, Encryption.TLS );
        httpsListenAddress = listenAddressFor( config, httpsConnector );
        httpsAdvertisedAddress = advertisedAddressFor( config, httpsConnector );

        database = new LifecycleManagingDatabase( config, graphFactory, dependencies );
        this.availabilityGuardSupplier = ((LifecycleManagingDatabase) database)::getAvailabilityGuard;
        life.add( database );
        life.add( new ServerDependenciesLifeCycleAdapter() );
        life.add( new ServerComponentsLifecycleAdapter() );
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

            throw translateToServerStartupError( t );
        }
    }

    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected DatabaseActions createDatabaseActions()
    {
        return new DatabaseActions( database.getGraph() );
    }

    private TransactionFacade createTransactionalActions()
    {
        final long timeoutMillis = getTransactionTimeoutMillis();
        final Clock clock = Clocks.systemClock();

        transactionRegistry = new TransactionHandleRegistry( clock, timeoutMillis, userLogProvider );

        // ensure that this is > 0
        long runEvery = round( timeoutMillis / 2.0 );

        resolveDependency( JobScheduler.class ).scheduleRecurring( Group.SERVER_TRANSACTION_TIMEOUT, () ->
        {
            long maxAge = clock.millis() - timeoutMillis;
            transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
        }, runEvery, MILLISECONDS );

        return new TransactionFacade(
                new TransitionalPeriodTransactionMessContainer( database.getGraph() ),
                resolveDependency( QueryExecutionEngine.class ),
                resolveDependency( GraphDatabaseQueryService.class ),
                transactionRegistry,
                userLogProvider
        );
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private long getTransactionTimeoutMillis()
    {
        final long timeout = config.get( ServerSettings.transaction_idle_timeout ).toMillis();
        return Math.max( timeout, MINIMUM_TIMEOUT + ROUNDING_SECOND );
    }

    /**
     * Use this method to register server modules from subclasses
     */
    protected final void registerModule( ServerModule module )
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
        new RunCarefully( map( module -> module::stop, serverModules ) ).run();
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
        webServer.setDefaultInjectables( createDefaultInjectables() );

        String sslPolicyName = config.get( ServerSettings.ssl_policy );
        if ( sslPolicyName != null )
        {
            SslPolicy sslPolicy = sslPolicyFactorySupplier.get().getPolicy( sslPolicyName );
            webServer.setSslPolicy( sslPolicy );
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
            log.info( "Remote interface available at %s", baseUri() );
        }
        catch ( Exception e )
        {
            ListenSocketAddress address = httpListenAddress != null ? httpListenAddress : httpsListenAddress;
            log.error( "Failed to start Neo4j on %s: %s", address, e.getMessage() );
            throw e;
        }
    }

    private void registerHttpAddressAfterStartup()
    {
        if ( httpConnector != null )
        {
            InetSocketAddress localHttpAddress = webServer.getLocalHttpAddress();
            connectorPortRegister.register( httpConnector.key(), localHttpAddress );
            if ( httpAdvertisedAddress.getPort() == 0 )
            {
                httpAdvertisedAddress = new AdvertisedSocketAddress( localHttpAddress.getHostString(), localHttpAddress.getPort() );
            }
        }
    }

    private void registerHttpsAddressAfterStartup()
    {
        if ( httpsConnector != null )
        {
            InetSocketAddress localHttpsAddress = webServer.getLocalHttpsAddress();
            connectorPortRegister.register( httpsConnector.key(), localHttpsAddress );
            if ( httpsAdvertisedAddress.getPort() == 0 )
            {
                httpsAdvertisedAddress = new AdvertisedSocketAddress( localHttpsAddress.getHostString(), localHttpsAddress.getPort() );
            }
        }
    }

    private void setUpHttpLogging() throws IOException
    {
        if ( !getConfig().get( http_logging_enabled ) )
        {
            return;
        }

        requestLog = new AsyncRequestLog(
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                config.get( db_timezone ).getZoneId(),
                config.get( http_log_path ).toString(),
                config.get( http_logging_rotation_size ),
                config.get( http_logging_rotation_keep_number ) );
        webServer.setRequestLog( requestLog );
    }

    protected Pattern[] getUriWhitelist()
    {
        return DEFAULT_URI_WHITELIST;
    }

    @Override
    public void stop()
    {
        tryShutdownAvailabiltyGuard();
        life.stop();
    }

    private void tryShutdownAvailabiltyGuard()
    {
        AvailabilityGuard guard = availabilityGuardSupplier.get();
        if ( guard != null )
        {
            guard.shutdown();
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
    public Database getDatabase()
    {
        return database;
    }

    @Override
    public TransactionRegistry getTransactionRegistry()
    {
        return transactionRegistry;
    }

    @Override
    public URI baseUri()
    {
        return httpAdvertisedAddress != null
               ? uriBuilder.buildURI( httpAdvertisedAddress, false )
               : uriBuilder.buildURI( httpsAdvertisedAddress, true );
    }

    public Optional<URI> httpsUri()
    {
        return Optional.ofNullable( httpsAdvertisedAddress )
                .map( address -> uriBuilder.buildURI( address, true ) );
    }

    public WebServer getWebServer()
    {
        return webServer;
    }

    @Override
    public PluginManager getExtensionManager()
    {
        RESTApiModule module = getModule( RESTApiModule.class );
        if ( module != null )
        {
            return module.getPlugins();
        }
        return null;
    }

    protected Collection<InjectableProvider<?>> createDefaultInjectables()
    {
        Collection<InjectableProvider<?>> singletons = new ArrayList<>();

        Database database = getDatabase();

        singletons.add( new DatabaseProvider( database ) );
        singletons.add( new DatabaseActions.Provider( databaseActions ) );
        singletons.add( new GraphDatabaseServiceProvider( database ) );
        singletons.add( new NeoServerProvider( this ) );
        singletons.add( providerForSingleton( new ConfigAdapter( getConfig() ), Configuration.class ) );
        singletons.add( providerForSingleton( getConfig(), Config.class ) );

        singletons.add( new WebServerProvider( getWebServer() ) );

        PluginInvocatorProvider pluginInvocatorProvider = new PluginInvocatorProvider( this );
        singletons.add( pluginInvocatorProvider );
        RepresentationFormatRepository repository = new RepresentationFormatRepository( this );

        singletons.add( new InputFormatProvider( repository ) );
        singletons.add( new OutputFormatProvider( repository ) );
        singletons.add( new CypherExecutorProvider( cypherExecutor ) );

        singletons.add( providerForSingleton( transactionFacade, TransactionFacade.class ) );
        singletons.add( providerFromSupplier( authManagerSupplier, AuthManager.class ) );
        singletons.add( providerFromSupplier( userManagerSupplier, UserManagerSupplier.class ) );
        singletons.add( new TransactionFilter( database ) );
        singletons.add( new LoggingProvider( userLogProvider ) );
        singletons.add( providerForSingleton( userLogProvider.getLog( NeoServer.class ), Log.class ) );
        singletons.add( providerForSingleton( resolveDependency( UsageData.class ), UsageData.class ) );

        return singletons;
    }

    @SuppressWarnings( "unchecked" )
    private <T extends ServerModule> T getModule( Class<T> clazz )
    {
        for ( ServerModule sm : serverModules )
        {
            if ( sm.getClass() == clazz )
            {
                return (T) sm;
            }
        }

        return null;
    }

    protected <T> T resolveDependency( Class<T> type )
    {
        return dependencyResolver.resolveDependency( type );
    }

    private static void verifyConnectorsConfiguration( Config config )
    {
        HttpConnector httpConnector = findConnector( config, Encryption.NONE );
        HttpConnector httpsConnector = findConnector( config, Encryption.TLS );

        if ( httpConnector == null && httpsConnector == null )
        {
            throw new IllegalArgumentException( "Either HTTP or HTTPS connector must be configured to run the server" );
        }
    }

    private static HttpConnector findConnector( Config config, Encryption encryption )
    {
        return config.enabledHttpConnectors()
                .stream()
                .filter( connector -> connector.encryptionLevel() == encryption )
                .findFirst()
                .orElse( null );
    }

    private static ListenSocketAddress listenAddressFor( Config config, HttpConnector connector )
    {
        return connector == null ? null : config.get( connector.listen_address );
    }

    private static AdvertisedSocketAddress advertisedAddressFor( Config config, HttpConnector connector )
    {
        return connector == null ? null : config.get( connector.advertised_address );
    }

    private class ServerDependenciesLifeCycleAdapter extends LifecycleAdapter
    {
        @Override
        public void start()
        {
            dependencyResolver = database.getGraph().getDependencyResolver();

            authManagerSupplier = dependencyResolver.provideDependency( AuthManager.class );
            userManagerSupplier = dependencyResolver.provideDependency( UserManagerSupplier.class );
            sslPolicyFactorySupplier = dependencyResolver.provideDependency( SslPolicyLoader.class );
            webServer = createWebServer();

            for ( ServerModule moduleClass : createServerModules() )
            {
                registerModule( moduleClass );
            }
        }
    }

    private class ServerComponentsLifecycleAdapter extends LifecycleAdapter
    {
        @Override
        public void start() throws Throwable
        {
            DiagnosticsManager diagnosticsManager = resolveDependency( DiagnosticsManager.class );
            Log diagnosticsLog = diagnosticsManager.getTargetLog();
            diagnosticsLog.info( "--- SERVER STARTED START ---" );
            connectorPortRegister = dependencyResolver.resolveDependency( ConnectorPortRegister.class );
            databaseActions = createDatabaseActions();

            transactionFacade = createTransactionalActions();

            cypherExecutor = new CypherExecutor( database, userLogProvider );

            configureWebServer();

            cypherExecutor.start();

            startModules();

            startWebServer();

            diagnosticsLog.info( "--- SERVER STARTED END ---" );
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
