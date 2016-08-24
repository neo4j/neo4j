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

import com.sun.jersey.api.core.HttpContext;
import org.apache.commons.configuration.Configuration;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.servlet.Filter;

import org.neo4j.bolt.security.ssl.Certificates;
import org.neo4j.bolt.security.ssl.KeyStoreFactory;
import org.neo4j.bolt.security.ssl.KeyStoreInformation;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.RunCarefully;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.CypherExecutorProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.guard.GuardingRequestFilter;
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
import org.neo4j.time.Clocks;
import org.neo4j.udc.UsageData;

import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.serverTransactionTimeout;
import static org.neo4j.server.configuration.ServerSettings.HttpConnector;
import static org.neo4j.server.configuration.ServerSettings.httpConnector;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_keep_number;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_size;
import static org.neo4j.server.database.InjectableProvider.providerForSingleton;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

public abstract class AbstractNeoServer implements NeoServer
{
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     *
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    private static final Pattern[] DEFAULT_URI_WHITELIST = new Pattern[]{
            Pattern.compile( "/browser.*" ),
            Pattern.compile( "/" )
    };

    private final Database.Factory dbFactory;
    private final GraphDatabaseFacadeFactory.Dependencies dependencies;
    protected final LogProvider logProvider;
    protected final Log log;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final Config config;
    private final LifeSupport life = new LifeSupport();
    private final HostnamePort httpAddress;
    private final Optional<HostnamePort> httpsAddress;

    protected Database database;
    protected CypherExecutor cypherExecutor;
    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    protected Optional<KeyStoreInformation> keyStoreInfo;
    private DatabaseActions databaseActions;
    private TransactionFacade transactionFacade;
    private TransactionHandleRegistry transactionRegistry;

    private boolean initialized = false;

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

        httpAddress = httpConnector( config, HttpConnector.Encryption.NONE )
                .orElseThrow( () ->
                        new IllegalArgumentException( "An HTTP connector must be configured to run the server" ) )
                .address
                .from( config );
        httpsAddress = httpConnector( config, HttpConnector.Encryption.TLS )
                .map( (connector) -> connector.address.from( config ) );
    }

    @Override
    public void init()
    {
        if ( initialized )
        {
            return;
        }

        this.database = life.add( dependencyResolver.satisfyDependency(dbFactory.newDatabase( config, dependencies)) );

        this.authManagerSupplier = dependencyResolver.provideDependency( AuthManager.class );
        this.webServer = createWebServer();

        this.keyStoreInfo = createKeyStore();

        for ( ServerModule moduleClass : createServerModules() )
        {
            registerModule( moduleClass );
        }

        this.initialized = true;
    }

    @Override
    public void start() throws ServerStartupException
    {
        init();
        try
        {
            life.start();

            DiagnosticsManager diagnosticsManager = resolveDependency(DiagnosticsManager.class);

            Log diagnosticsLog = diagnosticsManager.getTargetLog();
            diagnosticsLog.info( "--- SERVER STARTED START ---" );

            databaseActions = createDatabaseActions();

            transactionFacade = createTransactionalActions();

            cypherExecutor = new CypherExecutor( database );

            configureWebServer();

            cypherExecutor.start();

            startModules();

            startWebServer();

            diagnosticsLog.info( "--- SERVER STARTED END ---" );
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

        resolveDependency( JobScheduler.class ).scheduleRecurring( serverTransactionTimeout, () -> {
            long maxAge = clock.millis() - timeoutMillis;
            transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
        }, runEvery, MILLISECONDS );

        DependencyResolver dependencyResolver = database.getGraph().getDependencyResolver();
        return new TransactionFacade(
                new TransitionalPeriodTransactionMessContainer( database.getGraph() ),
                dependencyResolver.resolveDependency( QueryExecutionEngine.class ),
                dependencyResolver.resolveDependency( GraphDatabaseQueryService.class ), transactionRegistry, logProvider
        );
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private long getTransactionTimeoutMillis()
    {
        final long timeout = config.get( ServerSettings.transaction_timeout );
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

    // TODO: Once WebServer is fully implementing LifeCycle,
    // it should manage all but static (eg. unchangeable during runtime)
    // configuration itself.
    private void configureWebServer() throws Exception
    {
        webServer.setAddress( httpAddress );
        webServer.setHttpsAddress( httpsAddress );
        webServer.setMaxThreads( config.get( ServerSettings.webserver_max_threads ) );
        webServer.setWadlEnabled( config.get( ServerSettings.wadl_enabled ) );
        webServer.setDefaultInjectables( createDefaultInjectables() );
        if ( keyStoreInfo.isPresent() )
        {
            webServer.setHttpsCertificateInformation( keyStoreInfo.get() );
        }
    }

    private void startWebServer() throws Exception
    {
        try
        {
            setUpHttpLogging();
            setUpTimeoutFilter();
            webServer.start();
            log.info( "Remote interface available at %s", baseUri() );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start Neo4j on %s: %s", getAddress(), e.getMessage() );
            throw e;
        }
    }

    private void setUpHttpLogging() throws IOException {
        if ( !getConfig().get( http_logging_enabled ) )
        {
            return;
        }

        AsyncRequestLog requestLog = new AsyncRequestLog(
                new DefaultFileSystemAbstraction(),
                new File( config.get( GraphDatabaseSettings.logs_directory ), "http.log" ).toString(),
                config.get( http_logging_rotation_size ),
                config.get( http_logging_rotation_keep_number ) );
        webServer.setRequestLog( requestLog );
    }

    private void setUpTimeoutFilter()
    {
        if ( getConfig().get( ServerSettings.webserver_limit_execution_time ) == null )
        {
            return;
        }
        //noinspection deprecation
        Guard guard = resolveDependency( Guard.class );
        if ( guard == null )
        {
            throw new RuntimeException( format("Inconsistent configuration. In order to use %s, you must set %s.",
                    ServerSettings.webserver_limit_execution_time.name(),
                    GraphDatabaseSettings.execution_guard_enabled.name()) );
        }

        Filter filter = new GuardingRequestFilter( guard, getConfig().get( ServerSettings.webserver_limit_execution_time ) );
        webServer.addFilter( filter, "/*" );
    }

    public HostnamePort getAddress()
    {
        return httpAddress;
    }

    protected boolean httpsIsEnabled()
    {
        return httpsAddress.isPresent();
    }

    protected Pattern[] getUriWhitelist()
    {
        return DEFAULT_URI_WHITELIST;
    }

    protected Optional<KeyStoreInformation> createKeyStore()
    {
        if ( httpsIsEnabled() )
        {
            File privateKeyPath = config.get( ServerSettings.tls_key_file ).getAbsoluteFile();
            File certificatePath = config.get( ServerSettings.tls_certificate_file ).getAbsoluteFile();

            try
            {
                // If neither file is specified
                if ( (!certificatePath.exists() && !privateKeyPath.exists()) )
                {
                    //noinspection deprecation
                    log.info( "No SSL certificate found, generating a self-signed certificate.." );
                    Certificates certFactory = new Certificates();
                    certFactory.createSelfSignedCertificate( certificatePath, privateKeyPath, httpAddress.getHost() );
                }

                // Make sure both files were there, or were generated
                if ( !certificatePath.exists() )
                {
                    throw new ServerStartupException(
                            String.format(
                                    "TLS private key found, but missing certificate at '%s'. Cannot start server without certificate.",

                                    certificatePath ) );
                }
                if ( !privateKeyPath.exists() )
                {
                    throw new ServerStartupException(
                            String.format(
                                    "TLS certificate found, but missing key at '%s'. Cannot start server without key.",
                                    privateKeyPath ) );
                }

                return Optional.of( new KeyStoreFactory().createKeyStore( privateKeyPath, certificatePath ) );
            }
            catch ( GeneralSecurityException e )
            {
                throw new ServerStartupException(
                        "TLS certificate error occurred, unable to start server: " + e.getMessage(), e );
            }
            catch ( IOException | OperatorCreationException e )
            {
                throw new ServerStartupException(
                        "IO problem while loading or creating TLS certificates: " + e.getMessage(), e );
            }
        }
        else
        {
            return Optional.empty();
        }
    }

    @Override
    public void stop()
    {
        // TODO: All components should be moved over to the LifeSupport instance, life, in here.
        new RunCarefully(
                this::stopWebServer,
                this::stopModules,
                life::stop
        ).run();
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
        return uriBuilder.buildURI( httpAddress, false );
    }

    public Optional<URI> httpsUri()
    {
        return httpsAddress.map( ( address ) -> uriBuilder.buildURI( address, true ) );
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
        singletons.add( new AuthManagerProvider( authManagerSupplier ) );
        singletons.add( new TransactionFilter( database ) );
        singletons.add( new LoggingProvider( logProvider ) );
        singletons.add( providerForSingleton( logProvider.getLog( NeoServer.class ), Log.class ) );

        singletons.add( providerForSingleton( resolveDependency( UsageData.class ), UsageData.class ) );

        return singletons;
    }

    private static class AuthManagerProvider extends InjectableProvider<AuthManager>
    {
        private final Supplier<AuthManager> authManagerSupplier;
        private AuthManagerProvider( Supplier<AuthManager> authManagerSupplier )
        {
            super(AuthManager.class);
            this.authManagerSupplier = authManagerSupplier;
        }

        @Override
        public AuthManager getValue( HttpContext httpContext )
        {
            return authManagerSupplier.get();
        }
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

    @SuppressWarnings("unchecked")
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
}
