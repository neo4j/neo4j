/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.RunCarefully;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.configuration.HttpConnector.Encryption;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.CypherExecutorProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.ConfigAdapter;
import org.neo4j.server.plugins.PluginInvocatorProvider;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.rest.paging.LeaseManager;
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
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.scheduler.JobScheduler.Groups.serverTransactionTimeout;
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

    private final Database.Factory dbFactory;
    private final GraphDatabaseFacadeFactory.Dependencies dependencies;
    protected final LogProvider logProvider;
    protected final Log log;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final Config config;
    private final LifeSupport life = new LifeSupport();
    private final ListenSocketAddress httpListenAddress;
    private final Optional<ListenSocketAddress> httpsListenAddress;
    private AdvertisedSocketAddress httpAdvertisedAddress;
    private Optional<AdvertisedSocketAddress> httpsAdvertisedAddress;

    protected Database database;
    protected CypherExecutor cypherExecutor;
    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    protected Supplier<UserManagerSupplier> userManagerSupplier;
    protected Supplier<SslPolicyLoader> sslPolicyFactorySupplier;
    private DatabaseActions databaseActions;
    private TransactionFacade transactionFacade;

    private TransactionHandleRegistry transactionRegistry;
    private boolean initialized;
    private LifecycleAdapter serverComponents;
    protected ConnectorPortRegister connectorPortRegister;
    private HttpConnector httpConnector;
    private Optional<HttpConnector> httpsConnector;

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    public AbstractNeoServer( Config config, Database.Factory dbFactory,
            GraphDatabaseFacadeFactory.Dependencies dependencies, LogProvider logProvider )
    {
        this.config = config;
        this.dbFactory = dbFactory;
        this.dependencies = dependencies;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        log.info( NEO4J_IS_STARTING_MESSAGE );

        List<HttpConnector> httpConnectors = config.enabledHttpConnectors();

        httpConnector = httpConnectors.stream()
                .filter( c -> Encryption.NONE.equals( c.encryptionLevel() ) )
                .findFirst().orElseThrow( () ->
                        new IllegalArgumentException( "An HTTP connector must be configured to run the server" ) );

        httpListenAddress = config.get( httpConnector.listen_address );
        httpAdvertisedAddress = config.get( httpConnector.advertised_address );

        httpsConnector = httpConnectors.stream()
                .filter( c -> Encryption.TLS.equals( c.encryptionLevel() ) )
                .findFirst();
        httpsListenAddress = httpsConnector.map( connector -> config.get( connector.listen_address ) );
        httpsAdvertisedAddress = httpsConnector.map( connector -> config.get( connector.advertised_address ) );
    }

    @Override
    public void init()
    {
        if ( initialized )
        {
            return;
        }

        this.database =
                life.add( dependencyResolver.satisfyDependency( dbFactory.newDatabase( config, dependencies ) ) );

        this.authManagerSupplier = dependencyResolver.provideDependency( AuthManager.class );
        this.userManagerSupplier = dependencyResolver.provideDependency( UserManagerSupplier.class );
        this.sslPolicyFactorySupplier = dependencyResolver.provideDependency( SslPolicyLoader.class );
        this.webServer = createWebServer();

        for ( ServerModule moduleClass : createServerModules() )
        {
            registerModule( moduleClass );
        }

        serverComponents = new ServerComponentsLifecycleAdapter();
        life.add( serverComponents );

        this.initialized = true;
    }

    @Override
    public void start() throws ServerStartupException
    {
        init();
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
        return new DatabaseActions(
                new LeaseManager( Clocks.systemClock() ),
                config.get( ServerSettings.script_sandboxing_enabled ), database.getGraph() );
    }

    private TransactionFacade createTransactionalActions()
    {
        final long timeoutMillis = getTransactionTimeoutMillis();
        final Clock clock = Clocks.systemClock();

        transactionRegistry =
                new TransactionHandleRegistry( clock, timeoutMillis, logProvider );

        // ensure that this is > 0
        long runEvery = round( timeoutMillis / 2.0 );

        resolveDependency( JobScheduler.class ).scheduleRecurring( serverTransactionTimeout, () ->
        {
            long maxAge = clock.millis() - timeoutMillis;
            transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
        }, runEvery, MILLISECONDS );

        DependencyResolver dependencyResolver = database.getGraph().getDependencyResolver();
        return new TransactionFacade(
                new TransitionalPeriodTransactionMessContainer( database.getGraph() ),
                dependencyResolver.resolveDependency( QueryExecutionEngine.class ),
                dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ), transactionRegistry,
                logProvider
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

    @Override
    public Config getConfig()
    {
        return config;
    }

    private void configureWebServer() throws Exception
    {
        webServer.setAddress( httpListenAddress );
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

    private void startWebServer() throws Exception
    {
        try
        {
            setUpHttpLogging();
            webServer.start();
            InetSocketAddress localHttpAddress = webServer.getLocalHttpAddress();
            connectorPortRegister.register( httpConnector.key(), localHttpAddress );
            httpsConnector.ifPresent( connector -> connectorPortRegister.register( connector.key(), webServer
                    .getLocalHttpsAddress() ) );
            checkHttpAdvertisedAddress( localHttpAddress );
            checkHttpsAdvertisedAddress();
            log.info( "Remote interface available at %s", baseUri() );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start Neo4j on %s: %s", getAddress(), e.getMessage() );
            throw e;
        }
    }

    private void checkHttpsAdvertisedAddress()
    {
        httpsAdvertisedAddress.ifPresent( address ->
        {
            if ( address.getPort() == 0 )
            {
                InetSocketAddress localHttpsAddress = webServer.getLocalHttpsAddress();
                httpsAdvertisedAddress = Optional
                        .of( new AdvertisedSocketAddress( localHttpsAddress.getHostString(), localHttpsAddress.getPort() ) );
            }
        } );
    }

    private void checkHttpAdvertisedAddress( InetSocketAddress localHttpAddress )
    {
        if ( httpAdvertisedAddress.getPort() == 0 )
        {
            httpAdvertisedAddress = new AdvertisedSocketAddress( localHttpAddress.getHostString(), localHttpAddress.getPort() );
        }
    }

    private void setUpHttpLogging() throws IOException
    {
        if ( !getConfig().get( http_logging_enabled ) )
        {
            return;
        }

        AsyncRequestLog requestLog = new AsyncRequestLog(
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                config.get( http_log_path ).toString(),
                config.get( http_logging_rotation_size ),
                config.get( http_logging_rotation_keep_number ) );
        webServer.setRequestLog( requestLog );
    }

    public ListenSocketAddress getAddress()
    {
        return httpListenAddress;
    }

    protected boolean httpsIsEnabled()
    {
        return httpsListenAddress.isPresent();
    }

    protected Pattern[] getUriWhitelist()
    {
        return DEFAULT_URI_WHITELIST;
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    private void stopWebServer()
    {
        if ( webServer != null )
        {
            webServer.stop();
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
        return uriBuilder.buildURI( httpAdvertisedAddress, false );
    }

    public Optional<URI> httpsUri()
    {
        return httpsAdvertisedAddress.map( address -> uriBuilder.buildURI( address, true ) );
    }

    public WebServer getWebServer()
    {
        return webServer;
    }

    @Override
    public PluginManager getExtensionManager()
    {
        if ( hasModule( RESTApiModule.class ) )
        {
            return getModule( RESTApiModule.class ).getPlugins();
        }
        else
        {
            return null;
        }
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
        singletons.add( new LoggingProvider( logProvider ) );
        singletons.add( providerForSingleton( logProvider.getLog( NeoServer.class ), Log.class ) );

        singletons.add( providerForSingleton( resolveDependency( UsageData.class ), UsageData.class ) );

        return singletons;
    }

    private boolean hasModule( Class<? extends ServerModule> clazz )
    {
        for ( ServerModule sm : serverModules )
        {
            if ( sm.getClass() == clazz )
            {
                return true;
            }
        }
        return false;
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

    private final Dependencies dependencyResolver = new Dependencies( new Supplier<DependencyResolver>()
    {
        @Override
        public DependencyResolver get()
        {
            Database db = dependencyResolver.resolveDependency( Database.class );
            return db.getGraph().getDependencyResolver();
        }
    } );

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

            cypherExecutor = new CypherExecutor( database, logProvider );

            configureWebServer();

            cypherExecutor.start();

            startModules();

            startWebServer();

            diagnosticsLog.info( "--- SERVER STARTED END ---" );
        }

        @Override
        public void stop()
                throws Throwable
        {
            stopWebServer();
            stopModules();
        }
    }
}
