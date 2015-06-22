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

import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.Filter;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Provider;
import org.neo4j.helpers.RunCarefully;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.ConfigWrappingConfiguration;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.configuration.ConfigurationBuilder.ConfiguratorWrappingConfigurationBuilder;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.CypherExecutorProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.ExecutionEngineProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.database.RrdDbWrapper;
import org.neo4j.server.guard.GuardingRequestFilter;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.PluginInvocatorProvider;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.preflight.PreflightFailedException;
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
import org.neo4j.server.rrd.RrdDbProvider;
import org.neo4j.server.rrd.RrdFactory;
import org.neo4j.server.security.auth.AuthManager;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.server.security.ssl.KeyStoreFactory;
import org.neo4j.server.security.ssl.KeyStoreInformation;
import org.neo4j.server.security.ssl.SslCertificateFactory;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.web.WebServerProvider;
import org.neo4j.shell.ShellSettings;

import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.serverTransactionTimeout;
import static org.neo4j.server.database.InjectableProvider.providerForSingleton;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public abstract class AbstractNeoServer implements NeoServer
{
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     *
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    protected final InternalAbstractGraphDatabase.Dependencies dependencies;
    protected Database database;
    protected CypherExecutor cypherExecutor;
    protected ConfigurationBuilder configurator;
    protected WebServer webServer;

    protected AuthManager authManager;

    private final PreFlightTasks preFlight;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final Config dbConfig;
    private final LifeSupport life = new LifeSupport();

    private InterruptThreadTimer interruptStartupTimer;
    private DatabaseActions databaseActions;
    protected final ConsoleLogger log;

    private RoundRobinJobScheduler rrdDbScheduler;
    private RrdDbWrapper rrdDbWrapper;

    private TransactionFacade transactionFacade;
    private TransactionHandleRegistry transactionRegistry;

    protected abstract PreFlightTasks createPreflightTasks();

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    /**
     * Should use the new constructor with {@link ConfigurationBuilder}
     */
    @Deprecated
    public AbstractNeoServer( Configurator configurator, Database.Factory dbFactory, InternalAbstractGraphDatabase.Dependencies dependencies )
    {
        this( new ConfiguratorWrappingConfigurationBuilder( configurator ), dbFactory, dependencies );
    }

    public AbstractNeoServer( ConfigurationBuilder configurator, Database.Factory dbFactory, InternalAbstractGraphDatabase.Dependencies dependencies )
    {
        this.configurator = configurator;
        this.dependencies = dependencies;

        this.dbConfig = new Config();

        this.log = dependencies.logging().getConsoleLog( getClass() );

        this.database = life.add( dependencyResolver.satisfyDependency(dbFactory.newDatabase( dbConfig, dependencies)) );

        FileUserRepository users = life.add( new FileUserRepository( configurator.configuration().get( ServerInternalSettings.auth_store ).toPath(), dependencies.logging() ) );

        this.authManager = life.add(new AuthManager( users, Clock.SYSTEM_CLOCK, configurator.configuration().get( ServerSettings.auth_enabled ) ));
        this.preFlight = dependencyResolver.satisfyDependency(createPreflightTasks());
        this.webServer = createWebServer();

        for ( ServerModule moduleClass : createServerModules() )
        {
            registerModule( moduleClass );
        }
    }

    @Override
    public void init()
    {

    }

    @Override
    public void start() throws ServerStartupException
    {
        interruptStartupTimer = dependencyResolver.satisfyDependency(createInterruptStartupTimer());

        try
        {
            // Pre-flight tasks run outside the boot timeout limit
            runPreflightTasks();

            interruptStartupTimer.startCountdown();

            try
            {
                reloadConfigFromDisk();

                life.start();

                DiagnosticsManager diagnosticsManager = resolveDependency(DiagnosticsManager.class);

                StringLogger diagnosticsLog = diagnosticsManager.getTargetLog();
                diagnosticsLog.info( "--- SERVER STARTED START ---" );

                databaseActions = createDatabaseActions();

                if(getConfig().get( ServerInternalSettings.webadmin_enabled ))
                {
                    // TODO: RrdDb is not needed once we remove the old webadmin
                    rrdDbScheduler = new RoundRobinJobScheduler( dependencies.logging() );
                    rrdDbWrapper = new RrdFactory( configurator.configuration(), dependencies.logging() )
                            .createRrdDbAndSampler( database, rrdDbScheduler );
                }

                transactionFacade = createTransactionalActions();

                cypherExecutor = new CypherExecutor( database );

                configureWebServer();

                cypherExecutor.start();

                startModules();

                startWebServer();

                diagnosticsLog.info( "--- SERVER STARTED END ---" );
            }
            finally
            {
                interruptStartupTimer.stopCountdown();
            }
        }
        catch ( Throwable t )
        {
            // Make sure we don't leak rrd db files
            stopRrdDb();

            // If the database has been started, attempt to cleanly shut it down to avoid unclean shutdowns.
            life.shutdown();

            // Guard against poor operating systems that don't clear interrupt flags
            // after having handled interrupts (looking at you, Bill).
            Thread.interrupted();
            if ( interruptStartupTimer.wasTriggered() )
            {
                throw new ServerStartupException(
                        "Startup took longer than " + interruptStartupTimer.getTimeoutMillis() + "ms, " +
                                "and was stopped. You can disable this behavior by setting '" + ServerInternalSettings.startup_timeout.name() + "' to 0.",
                        1 );
            }

            throw new ServerStartupException( format( "Starting Neo4j Server failed: %s", t.getMessage() ), t );
        }
    }

    private void reloadConfigFromDisk()
    {
        Map<String, String> result = new HashMap<>( configurator.getDatabaseTuningProperties() );
        result.put( GraphDatabaseSettings.store_dir.name(), configurator.configuration()
                .get( ServerInternalSettings.legacy_db_location ).getAbsolutePath() );

        // make sure that the default in server for db_query_log_filename is "data/log/queries.log" instead of null
        if ( dbConfig.get( GraphDatabaseSettings.log_queries_filename ) == null )
        {
            result.put( GraphDatabaseSettings.log_queries_filename.name(), "data/log/queries.log" );
        }

        putIfAbsent( result, ShellSettings.remote_shell_enabled.name(), Settings.TRUE );

        result.putAll( configurator.configuration().getParams() );

        dbConfig.applyChanges( result );
    }

    private void putIfAbsent( Map<String, String> databaseProperties, String configKey, String configValue )
    {
        if ( databaseProperties.get( configKey ) == null )
        {
            databaseProperties.put( configKey, configValue );
        }
    }

    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected DatabaseActions createDatabaseActions()
    {
        return new DatabaseActions(
                new LeaseManager( SYSTEM_CLOCK ),
                configurator.configuration().get(
                        ServerInternalSettings.script_sandboxing_enabled ), database.getGraph() );
    }

    private TransactionFacade createTransactionalActions()
    {
        final long timeoutMillis = getTransactionTimeoutMillis();
        final Clock clock = SYSTEM_CLOCK;

        transactionRegistry =
            new TransactionHandleRegistry( clock, timeoutMillis, dependencies.logging().getMessagesLog(TransactionRegistry.class) );

        // ensure that this is > 0
        long runEvery = round( timeoutMillis / 2.0 );

        resolveDependency( JobScheduler.class ).scheduleRecurring( serverTransactionTimeout, new Runnable()
        {
            @Override
            public void run()
            {
                long maxAge = clock.currentTimeMillis() - timeoutMillis;
                transactionRegistry.rollbackSuspendedTransactionsIdleSince( maxAge );
            }
        }, runEvery, MILLISECONDS );

        return new TransactionFacade(
                new TransitionalPeriodTransactionMessContainer( database.getGraph() ),
                database.getGraph().getDependencyResolver().resolveDependency( QueryExecutionEngine.class ),
                transactionRegistry,
                dependencies.logging().getMessagesLog( TransactionFacade.class )
        );
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private long getTransactionTimeoutMillis()
    {
        final long timeout = configurator.configuration().get( ServerSettings.transaction_timeout );
        return Math.max( timeout, MINIMUM_TIMEOUT + ROUNDING_SECOND );
    }

    protected InterruptThreadTimer createInterruptStartupTimer()
    {
        long startupTimeout = configurator.configuration().get( ServerInternalSettings.startup_timeout );
        InterruptThreadTimer stopStartupTimer;
        if ( startupTimeout > 0 )
        {
            log.log( "Setting startup timeout to: " + startupTimeout + "ms based on "
                    + configurator.configuration().get( ServerInternalSettings.startup_timeout ) );
            stopStartupTimer = InterruptThreadTimer.createTimer(
                    startupTimeout,
                    Thread.currentThread() );
        }
        else
        {
            stopStartupTimer = InterruptThreadTimer.createNoOpTimer();
        }
        return stopStartupTimer;
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
        new RunCarefully( map( new Function<ServerModule, Runnable>()
        {
            @Override
            public Runnable apply( final ServerModule module )
            {
                return new Runnable()
                {
                    @Override
                    public void run()
                    {
                        module.stop();
                    }
                };
            }
        }, serverModules ) )
                .run();
    }

    private void runPreflightTasks()
    {
        if ( !preFlight.run() )
        {
            throw new PreflightFailedException( preFlight.failedTask() );
        }
    }

    @Override
    public Config getConfig()
    {
        return configurator.configuration();
    }

    @Override
    public Configuration getConfiguration()
    {
        return new ConfigWrappingConfiguration( this.configurator.configuration() );
    }

    protected Logging getLogging()
    {
        return dependencies.logging();
    }

    // TODO: Once WebServer is fully implementing LifeCycle,
    // it should manage all but static (eg. unchangeable during runtime)
    // configuration itself.
    private void configureWebServer()
    {
        int webServerPort = getWebServerPort();
        String webServerAddr = getWebServerAddress();

        int maxThreads = getMaxThreads();

        int sslPort = getHttpsPort();
        boolean sslEnabled = getHttpsEnabled();

        log.log( "Starting HTTP on port :%s with %d threads available", webServerPort, maxThreads );
        webServer.setPort( webServerPort );
        webServer.setAddress( webServerAddr );
        webServer.setMaxThreads( maxThreads );

        webServer.setEnableHttps( sslEnabled );
        webServer.setHttpsPort( sslPort );

        webServer.setWadlEnabled( configurator.configuration().get( ServerInternalSettings.wadl_enabled ) );
        webServer.setDefaultInjectables( createDefaultInjectables() );

        if ( sslEnabled )
        {
            log.log( "Enabling HTTPS on port :%s", sslPort );
            webServer.setHttpsCertificateInformation( initHttpsKeyStore() );
        }
    }

    private int getMaxThreads()
    {
        return configurator.configuration()
                .get( ServerSettings.webserver_max_threads ) != null ? configurator.configuration()
                .get( ServerSettings.webserver_max_threads ) : defaultMaxWebServerThreads();
    }

    private int defaultMaxWebServerThreads()
    {
        return Math.min(Runtime.getRuntime()
                .availableProcessors(),500);
    }

    private void startWebServer()
    {
        try
        {
            setUpHttpLogging();

            setUpTimeoutFilter();

            webServer.start();

            log.log( "Server started on: " + baseUri() );

            //noinspection deprecation
            log.log( format( "Remote interface ready and available at [%s]", baseUri() ) );
        }
        catch ( RuntimeException e )
        {
            //noinspection deprecation
            log.error( format( "Failed to start Neo Server on port [%d], reason [%s]",
                    getWebServerPort(), e.getMessage() ) );
            throw e;
        }
    }

    private void setUpHttpLogging()
    {
        if ( !httpLoggingProperlyConfigured() )
        {
            return;
        }
        boolean contentLoggingEnabled = configurator.configuration().get( ServerSettings.http_logging_enabled );

        File logFile = configurator.configuration().get( ServerSettings.http_log_config_File );
        webServer.setHttpLoggingConfiguration( logFile, contentLoggingEnabled );
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

    private boolean httpLoggingProperlyConfigured()
    {
        return loggingEnabled() && configLocated();
    }

    private boolean configLocated()
    {
        final File logFile = getConfig().get( ServerSettings.http_log_config_File );
        return logFile != null && logFile.exists();
    }

    private boolean loggingEnabled()
    {
        return getConfig().get( ServerSettings.http_logging_enabled );
    }

    protected int getWebServerPort()
    {
        return configurator.configuration().get( ServerSettings.webserver_port );
    }

    protected boolean getHttpsEnabled()
    {
        return configurator.configuration().get( ServerSettings.webserver_https_enabled );
    }

    protected int getHttpsPort()
    {
        return configurator.configuration().get( ServerSettings.webserver_https_port );
    }

    protected String getWebServerAddress()
    {
        return configurator.configuration().get( ServerSettings.webserver_address );
    }

    // TODO: This is jetty-specific, move to Jetty9WebServer

    /**
     * Jetty wants certificates stored in a key store, which is nice, but
     * to make it easier for non-java savvy users, we let them put
     * their certificates directly on the file system (advising appropriate
     * permissions etc), like you do with Apache Web Server. On each startup
     * we set up a key store for them with their certificate in it.
     */
    protected KeyStoreInformation initHttpsKeyStore()
    {
        File keystorePath = configurator.configuration().get( ServerSettings.webserver_keystore_path );

        File privateKeyPath = configurator.configuration().get( ServerSettings.webserver_https_key_path );

        File certificatePath = configurator.configuration().get( ServerSettings.webserver_https_cert_path );

        if ( !certificatePath.exists() )
        {
            //noinspection deprecation
            log.log( "No SSL certificate found, generating a self-signed certificate.." );
            SslCertificateFactory certFactory = new SslCertificateFactory();
            certFactory.createSelfSignedCertificate( certificatePath, privateKeyPath, getWebServerAddress() );
        }

        KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
        return keyStoreFactory.createKeyStore( keystorePath, privateKeyPath, certificatePath );
    }

    @Override
    public void stop()
    {
        // TODO: All components should be moved over to the LifeSupport instance, life, in here.
        new RunCarefully(
            new Runnable() {
                @Override
                public void run()
                {
                    stopWebServer();
                }
            },
            new Runnable() {
                @Override
                public void run()
                {
                    stopModules();
                }
            },
            new Runnable() {
                @Override
                public void run()
                {
                    stopRrdDb();
                }
            },
            new Runnable() {
                @Override
                public void run()
                {
                    life.stop();
                }
            }
        ).run();

        //noinspection deprecation
        log.log( "Successfully shutdown database." );
    }

    private void stopRrdDb()
    {
        try
        {
            if( rrdDbScheduler != null)
            {
                rrdDbScheduler.stopJobs();
            }
            if( rrdDbWrapper != null )
            {
                rrdDbWrapper.close();
            }
            log.log( "Successfully shutdown Neo4j Server." );
        } catch(IOException e)
        {
            // If we fail on shutdown, we can't really recover from it. Log the issue and carry on.
            log.error( "Unable to cleanly shut down statistics database.", e );
        }
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
        return uriBuilder.buildURI( getWebServerAddress(), getWebServerPort(), false );
    }

    public URI httpsUri()
    {
        return uriBuilder.buildURI( getWebServerAddress(), getHttpsPort(), true );
    }

    public WebServer getWebServer()
    {
        return webServer;
    }

    @Override
    public ConfigurationBuilder getConfigurationBuilder()
    {
        return configurator;
    }

    @Override
    public Configurator getConfigurator()
    {
        return new ConfigurationBuilder.ConfigurationBuilderWrappingConfigurator( getConfigurationBuilder() );
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
        singletons.add( providerForSingleton( new ConfigWrappingConfiguration( getConfig() ), Configuration.class ) );
        singletons.add( providerForSingleton( getConfig(), Config.class ) );

        if(getConfig().get( ServerInternalSettings.webadmin_enabled ))
        {
            singletons.add( new RrdDbProvider( rrdDbWrapper ) );
        }

        singletons.add( new WebServerProvider( getWebServer() ) );

        PluginInvocatorProvider pluginInvocatorProvider = new PluginInvocatorProvider( this );
        singletons.add( pluginInvocatorProvider );
        RepresentationFormatRepository repository = new RepresentationFormatRepository( this );

        singletons.add( new InputFormatProvider( repository ) );
        singletons.add( new OutputFormatProvider( repository ) );
        singletons.add( new CypherExecutorProvider( cypherExecutor ) );
        singletons.add( new ExecutionEngineProvider( cypherExecutor ) );

        singletons.add( providerForSingleton( transactionFacade, TransactionFacade.class ) );
        singletons.add( providerForSingleton( authManager, AuthManager.class ) );
        singletons.add( new TransactionFilter( database ) );
        singletons.add( new LoggingProvider( dependencies.logging() ) );
        singletons.add( providerForSingleton( dependencies.logging().getConsoleLog( NeoServer.class ), ConsoleLogger.class ) );

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

    private final Dependencies dependencyResolver = new Dependencies(new Provider<DependencyResolver>()
    {
        @Override
        public DependencyResolver instance()
        {
            Database db = dependencyResolver.resolveDependency( Database.class );
            return db.getGraph().getDependencyResolver();
        }
    });
}
