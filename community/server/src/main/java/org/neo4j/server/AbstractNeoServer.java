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

import org.apache.commons.configuration.Configuration;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import javax.servlet.Filter;

import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.RunCarefully;
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
import org.neo4j.server.configuration.ConfigWrappingConfiguration;
import org.neo4j.server.configuration.ConfigurationBuilder;
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
import org.neo4j.server.security.ssl.Certificates;
import org.neo4j.server.security.ssl.KeyStoreFactory;
import org.neo4j.server.security.ssl.KeyStoreInformation;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.web.WebServerProvider;

import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.Clock.SYSTEM_CLOCK;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.serverTransactionTimeout;
import static org.neo4j.server.configuration.ServerSettings.http_log_config_file;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.database.InjectableProvider.providerForSingleton;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

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

    private static final Pattern[] DEFAULT_URI_WHITELIST = new Pattern[]{
            Pattern.compile( "/browser.*" ),
            Pattern.compile( "/webadmin.*" ),
            Pattern.compile( "/" )
    };

    private final Database.Factory dbFactory;
    private final GraphDatabaseFacadeFactory.Dependencies dependencies;
    protected final LogProvider logProvider;
    protected final Log log;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final LifeSupport life = new LifeSupport();
    private Config config;

    protected Database database;
    protected CypherExecutor cypherExecutor;
    protected WebServer webServer;

    protected AuthManager authManager;
    protected KeyStoreInformation keyStoreInfo;

    private DatabaseActions databaseActions;

    private RoundRobinJobScheduler rrdDbScheduler;
    private RrdDbWrapper rrdDbWrapper;

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
    }

    @Override
    public void init()
    {
        if ( initialized )
        {
            return;
        }

        this.database = life.add( dependencyResolver.satisfyDependency(dbFactory.newDatabase( config, dependencies)) );

        FileUserRepository users = life.add( new FileUserRepository( config.get( ServerInternalSettings.auth_store ).toPath(), logProvider ) );

        this.authManager = life.add(new AuthManager( users, Clock.SYSTEM_CLOCK, config.get( ServerSettings.auth_enabled ) ));
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

            if( getConfig().get( ServerInternalSettings.webadmin_enabled ) &&
                getConfig().get( ServerInternalSettings.rrdb_enabled ) )
            {
                // TODO: RrdDb is not needed once we remove the old webadmin
                rrdDbScheduler = new RoundRobinJobScheduler( logProvider );
                rrdDbWrapper = new RrdFactory( config, logProvider )
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
        catch ( Throwable t )
        {
            // Make sure we don't leak rrd db files
            stopRrdDb();

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
                new LeaseManager( SYSTEM_CLOCK ),
                config.get( ServerInternalSettings.script_sandboxing_enabled ), database.getGraph() );
    }

    private TransactionFacade createTransactionalActions()
    {
        final long timeoutMillis = getTransactionTimeoutMillis();
        final Clock clock = SYSTEM_CLOCK;

        transactionRegistry =
            new TransactionHandleRegistry( clock, timeoutMillis, logProvider );

        // ensure that this is > 0
        long runEvery = round( timeoutMillis / 2.0 );

        resolveDependency( JobScheduler.class ).scheduleRecurring( serverTransactionTimeout, new
                Runnable()
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
                transactionRegistry, logProvider
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

    @Override
    public Config getConfig()
    {
        return config;
    }

    @Override
    public Configuration getConfiguration()
    {
        return new ConfigWrappingConfiguration( config );
    }

    // TODO: Once WebServer is fully implementing LifeCycle,
    // it should manage all but static (eg. unchangeable during runtime)
    // configuration itself.
    private void configureWebServer() throws Exception
    {
        int webServerPort = getWebServerPort();
        String webServerAddr = getWebServerAddress();

        int maxThreads = config.get( ServerSettings.webserver_max_threads );

        int sslPort = getHttpsPort();
        boolean sslEnabled = getHttpsEnabled();

        log.info( "Starting HTTP on port %s (%d threads available)", webServerPort, maxThreads );
        webServer.setPort( webServerPort );
        webServer.setAddress( webServerAddr );
        webServer.setMaxThreads( maxThreads );

        webServer.setEnableHttps( sslEnabled );
        webServer.setHttpsPort( sslPort );

        webServer.setWadlEnabled( config.get( ServerInternalSettings.wadl_enabled ) );
        webServer.setDefaultInjectables( createDefaultInjectables() );

        if ( sslEnabled )
        {
            log.info( "Enabling HTTPS on port %s", sslPort );
            webServer.setHttpsCertificateInformation( keyStoreInfo );
        }
    }

    private void startWebServer() throws Exception
    {
        try
        {
            setUpHttpLogging();

            setUpTimeoutFilter();

            webServer.start();

            log.info( "Remote interface ready and available at %s", baseUri() );
        }
        catch ( Exception e )
        {
            //noinspection deprecation
            log.error( "Failed to start Neo Server on port %d: %s",
                    getWebServerPort(), e.getMessage() );
            throw e;
        }
    }

    private void setUpHttpLogging()
    {
        if ( !httpLoggingProperlyConfigured() )
        {
            return;
        }

        webServer.setHttpLoggingConfiguration(config.get( http_log_config_file ), config.get( http_logging_enabled ));
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
        final File logFile = getConfig().get( http_log_config_file );
        return logFile != null && logFile.exists();
    }

    private boolean loggingEnabled()
    {
        return getConfig().get( http_logging_enabled );
    }

    protected int getWebServerPort()
    {
        return config.get( ServerSettings.webserver_port );
    }

    protected boolean getHttpsEnabled()
    {
        return config.get( ServerSettings.webserver_https_enabled );
    }

    protected int getHttpsPort()
    {
        return config.get( ServerSettings.webserver_https_port );
    }

    protected String getWebServerAddress()
    {
        return config.get( ServerSettings.webserver_address );
    }

    protected Pattern[] getUriWhitelist()
    {
        return DEFAULT_URI_WHITELIST;
    }

    protected KeyStoreInformation createKeyStore()
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
                certFactory.createSelfSignedCertificate( certificatePath, privateKeyPath, getWebServerAddress() );
            }

            // Make sure both files were there, or were generated
            if( !certificatePath.exists() )
            {
                throw new ServerStartupException(
                        String.format("TLS private key found, but missing certificate at '%s'. Cannot start server without certificate.", certificatePath ) );
            }
            if( !privateKeyPath.exists() )
            {
                throw new ServerStartupException(
                        String.format("TLS certificate found, but missing key at '%s'. Cannot start server without key.", privateKeyPath ) );
            }

            return new KeyStoreFactory().createKeyStore( privateKeyPath, certificatePath );
        }
        catch( GeneralSecurityException e )
        {
            throw new ServerStartupException( "TLS certificate error occurred, unable to start server: " + e.getMessage(), e );
        }
        catch( IOException | OperatorCreationException e )
        {
            throw new ServerStartupException( "IO problem while loading or creating TLS certificates: " + e.getMessage(), e );
        }
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
        log.info( "Successfully shutdown database" );
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
            log.info( "Successfully shutdown Neo4j Server" );
        } catch(IOException e)
        {
            // If we fail on shutdown, we can't really recover from it. Log the issue and carry on.
            log.error( "Unable to cleanly shut down statistics database", e );
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
    public Configurator getConfigurator()
    {
        return new ConfigurationBuilder.ConfigWrappingConfigurator( config );
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

        if (  getConfig().get( ServerInternalSettings.webadmin_enabled ) &&
              getConfig().get( ServerInternalSettings.rrdb_enabled ) )
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
        singletons.add( new LoggingProvider( logProvider ) );
        singletons.add( providerForSingleton( logProvider.getLog( NeoServer.class ), Log.class ) );

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

    private final Dependencies dependencyResolver = new Dependencies(new Supplier<DependencyResolver>()
    {
        @Override
        public DependencyResolver get()
        {
            Database db = dependencyResolver.resolveDependency( Database.class );
            return db.getGraph().getDependencyResolver();
        }
    });
}
