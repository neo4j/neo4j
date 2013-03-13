/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.helpers.collection.Iterables.option;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.ConfigurationProvider;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.CypherExecutor;
import org.neo4j.server.database.CypherExecutorProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseProvider;
import org.neo4j.server.database.GraphDatabaseServiceProvider;
import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.PluginInvocatorProvider;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.preflight.PreflightFailedException;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.paging.RealClock;
import org.neo4j.server.rest.repr.InputFormatProvider;
import org.neo4j.server.rest.repr.OutputFormatProvider;
import org.neo4j.server.rest.repr.RepresentationFormatRepository;
import org.neo4j.server.rest.web.DatabaseActions;
import org.neo4j.server.rrd.RrdDbProvider;
import org.neo4j.server.security.KeyStoreFactory;
import org.neo4j.server.security.KeyStoreInformation;
import org.neo4j.server.security.SslCertificateFactory;
import org.neo4j.server.statistic.StatisticCollector;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.web.WebServerProvider;

public abstract class AbstractNeoServer implements NeoServer
{
    public static final Logger log = Logger.getLogger( AbstractNeoServer.class );

    protected Database database;
    protected CypherExecutor cypherExecutor;
    protected Configurator configurator;
    protected WebServer webServer;

    protected final StatisticCollector statisticsCollector = new StatisticCollector();

    private PreFlightTasks preflight;
    private final List<ServerModule> serverModules = new ArrayList<ServerModule>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private InterruptThreadTimer interruptStartupTimer;
    private DatabaseActions databaseActions;

    private final DependencyResolver dependencyResolver = new DependencyResolver.Adapter()
    {
        private <T> T resolveKnownSingleDependency( Class<T> type )
        {
            if ( type.equals( Database.class ) )
            {
                return (T) database;
            }
            else if ( type.equals( PreFlightTasks.class ) )
            {
                return (T) preflight;
            }
            else if ( type.equals( InterruptThreadTimer.class ) )
            {
                return (T) interruptStartupTimer;
            }
            else if ( type.equals( Logging.class ) )
            {
                // TODO logging should be owned by server, waiting for logging refactoring
                DependencyResolver kernelDependencyResolver = database.getGraph().getDependencyResolver();
                return (T) kernelDependencyResolver.resolveDependency( Logging.class );
            }
            return null;
        }
        
        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy<T> selector )
        {
            return selector.select( type, option( resolveKnownSingleDependency( type ) ) );
        }
    };

    protected abstract PreFlightTasks createPreflightTasks();

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract Database createDatabase();

    protected abstract WebServer createWebServer();

    protected DatabaseActions createDatabaseActions()
    {
        return new DatabaseActions( database,
                new LeaseManager( new RealClock() ),
                ForceMode.forced,
                configurator.configuration().getBoolean(
                        Configurator.SCRIPT_SANDBOXING_ENABLED_KEY,
                        Configurator.DEFAULT_SCRIPT_SANDBOXING_ENABLED ) );
    }

    @Override
    public void init()
    {
        this.preflight = createPreflightTasks();
        this.database = createDatabase();
        this.webServer = createWebServer();

        for ( ServerModule moduleClass : createServerModules() )
        {
            registerModule( moduleClass );
        }
    }

    protected Logging getLogging()
    {
        return dependencyResolver.resolveDependency( Logging.class );
    }

    @Override
    public void start() throws ServerStartupException
    {
        interruptStartupTimer = createInterruptStartupTimer();

        try
        {
            // Pre-flight tasks run outside the boot timeout limit
            runPreflightTasks();

            interruptStartupTimer.startCountdown();

            database.start();

            databaseActions = createDatabaseActions();

            cypherExecutor = new CypherExecutor( database, getLogging().getLogger( CypherExecutor.class ) );

            configureWebServer();

            cypherExecutor.start();

            DiagnosticsManager diagnosticsManager = database.getGraph().getDiagnosticsManager();

            StringLogger logger = diagnosticsManager.getTargetLog();
            logger.logMessage( "--- SERVER STARTED START ---" );

            diagnosticsManager.register( Configurator.DIAGNOSTICS, configurator );

            startModules( logger );

            startWebServer( logger );

            logger.logMessage( "--- SERVER STARTED END ---", true );

            interruptStartupTimer.stopCountdown();

        }
        catch ( Throwable t )
        {
            // Make sure this does not trigger interrupts outside of this method.
            interruptStartupTimer.stopCountdown();

            // Guard against poor operating systems that don't clear interrupt flags
            // after having handled interrupts (looking at you, Bill).
            Thread.interrupted();

            if ( interruptStartupTimer.wasTriggered() )
            {
                throw new ServerStartupException(
                        "Startup took longer than " + interruptStartupTimer.getTimeoutMillis() + "ms, " +
                                "and was stopped. You can disable this behavior by setting '" + Configurator
                                .STARTUP_TIMEOUT + "' to 0.",
                        1 );
            }

            if ( t instanceof RuntimeException )
            {
                throw (RuntimeException) t;
            }
            else
            {
                throw new ServerStartupException( "Starting neo server failed, see nested exception.", t );
            }
        }
    }

    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    protected InterruptThreadTimer createInterruptStartupTimer()
    {
        long startupTimeout = getConfiguration().getInt( Configurator.STARTUP_TIMEOUT,
                Configurator.DEFAULT_STARTUP_TIMEOUT ) * 1000;
        InterruptThreadTimer stopStartupTimer;
        if ( startupTimeout > 0 )
        {
            log.info( "Setting startup timeout to: " + startupTimeout + "ms based on " + getConfiguration().getInt(
                    Configurator.STARTUP_TIMEOUT, -1 ) );
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
     *
     * @param module
     */
    protected final void registerModule( ServerModule module )
    {
        serverModules.add( module );
    }

    private void startModules( StringLogger logger )
    {
        for ( ServerModule module : serverModules )
        {
            module.start( logger );
        }
    }

    private void stopModules()
    {
        for ( ServerModule module : serverModules )
        {

            try
            {
                module.stop();
            }
            catch ( Exception e )
            {
                log.error( e );
            }
        }
    }

    private void runPreflightTasks()
    {
        if ( !preflight.run() )
        {
            throw new PreflightFailedException( preflight.failedTask() );
        }
    }

    @Override
    public Configuration getConfiguration()
    {
        return configurator.configuration();
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

        log.info( "Starting Neo Server on port [%s] with [%d] threads available", webServerPort, maxThreads );
        webServer.setPort( webServerPort );
        webServer.setAddress( webServerAddr );
        webServer.setMaxThreads( maxThreads );

        webServer.setEnableHttps( sslEnabled );
        webServer.setHttpsPort( sslPort );

        webServer.setWadlEnabled(
                Boolean.valueOf( String.valueOf( getConfiguration().getProperty( Configurator.WADL_ENABLED ) ) ) );
        webServer.setDefaultInjectables( createDefaultInjectables() );

        if ( sslEnabled )
        {
            log.info( "Enabling HTTPS on port [%s]", sslPort );
            webServer.setHttpsCertificateInformation( initHttpsKeyStore() );
        }
    }

    private int getMaxThreads()
    {
        return configurator.configuration()
                .containsKey( Configurator.WEBSERVER_MAX_THREADS_PROPERTY_KEY ) ? configurator.configuration()
                .getInt( Configurator.WEBSERVER_MAX_THREADS_PROPERTY_KEY ) : defaultMaxWebServerThreads();
    }

    private int defaultMaxWebServerThreads()
    {
        return 10 * Runtime.getRuntime()
                .availableProcessors();
    }

    private void startWebServer( StringLogger logger )
    {
        try
        {
            if ( httpLoggingProperlyConfigured() )
            {
                webServer.setHttpLoggingConfiguration(
                        new File( getConfiguration().getProperty( Configurator.HTTP_LOG_CONFIG_LOCATION ).toString()
                        ) );
            }

            webServer.start();

            Integer limit = getConfiguration().getInteger( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY,
                    null );
            if ( limit != null )
            {
                webServer.addExecutionLimitFilter( limit, database.getGraph().getGuard() );
            }

            if ( logger != null )
            {
                logger.logMessage( "Server started on: " + baseUri() );
            }
            log.info( "Server started on [%s]", baseUri() );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            log.error( "Failed to start Neo Server on port [%d], reason [%s]", getWebServerPort(), e.getMessage() );
        }
    }


    private boolean httpLoggingProperlyConfigured()
    {
        return loggingEnabled() && configLocated();
    }

    private boolean configLocated()
    {
        final Object property = getConfiguration().getProperty( Configurator.HTTP_LOG_CONFIG_LOCATION );
        return property != null && new File( String.valueOf( property ) ).exists();
    }

    private boolean loggingEnabled()
    {
        return "true".equals( String.valueOf( getConfiguration().getProperty( Configurator.HTTP_LOGGING ) ) );
    }

    protected int getWebServerPort()
    {
        return configurator.configuration()
                .getInt( Configurator.WEBSERVER_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_PORT );
    }

    protected boolean getHttpsEnabled()
    {
        return configurator.configuration()
                .getBoolean( Configurator.WEBSERVER_HTTPS_ENABLED_PROPERTY_KEY,
                        Configurator.DEFAULT_WEBSERVER_HTTPS_ENABLED );
    }

    protected int getHttpsPort()
    {
        return configurator.configuration()
                .getInt( Configurator.WEBSERVER_HTTPS_PORT_PROPERTY_KEY, Configurator.DEFAULT_WEBSERVER_HTTPS_PORT );
    }

    protected String getWebServerAddress()
    {
        return configurator.configuration()
                .getString( Configurator.WEBSERVER_ADDRESS_PROPERTY_KEY,
                        Configurator.DEFAULT_WEBSERVER_ADDRESS );
    }

    // TODO: This is jetty-specific, move to Jetty6WebServer

    /**
     * Jetty wants certificates stored in a key store, which is nice, but
     * to make it easier for non-java savvy users, we let them put
     * their certificates directly on the file system (advicing apropriate
     * permissions etc), like you do with Apache Web Server. On each startup
     * we set up a key store for them with their certificate in it.
     */
    protected KeyStoreInformation initHttpsKeyStore()
    {
        File keystorePath = new File( configurator.configuration().getString(
                Configurator.WEBSERVER_KEYSTORE_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_KEYSTORE_PATH ) );

        File privateKeyPath = new File( configurator.configuration().getString(
                Configurator.WEBSERVER_HTTPS_KEY_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_HTTPS_KEY_PATH ) );

        File certificatePath = new File( configurator.configuration().getString(
                Configurator.WEBSERVER_HTTPS_CERT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_WEBSERVER_HTTPS_CERT_PATH ) );

        if ( !certificatePath.exists() )
        {
            log.info( "No SSL certificate found, generating a self-signed certificate.." );
            SslCertificateFactory certFactory = new SslCertificateFactory();
            certFactory.createSelfSignedCertificate( certificatePath, privateKeyPath, getWebServerAddress() );
        }

        KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
        return keyStoreFactory.createKeyStore( keystorePath, privateKeyPath, certificatePath );
    }

    @Override
    public void stop()
    {
        try
        {
            stopServerOnly();
            stopDatabase();
            log.info( "Successfully shutdown database." );
        }
        catch ( Exception e )
        {
            log.warn( "Failed to cleanly shutdown database." );
        }
    }

    /**
     * Stops everything but the database.
     * <p/>
     * This is deprecated. If you would like to disconnect the database
     * life cycle from server control, then use {@link WrappingNeoServer}.
     * <p/>
     * To stop the server, please use {@link #stop()}.
     * <p/>
     * This will be removed in 1.10
     */
    @Deprecated
    public void stopServerOnly()
    {
        try
        {
            stopWebServer();
            stopModules();
            log.info( "Successfully shutdown Neo4j Server." );
        }
        catch ( Exception e )
        {
            log.warn( "Failed to cleanly shutdown Neo4j Server." );
        }
    }

    private void stopWebServer()
    {
        if ( webServer != null )
        {
            webServer.stop();
        }
    }

    private void stopDatabase()
    {
        if ( database != null )
        {
            try
            {
                database.stop();
            }
            catch ( Throwable e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    @Override
    public Database getDatabase()
    {
        return database;
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
        return configurator;
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
        Collection<InjectableProvider<?>> singletons = new ArrayList<InjectableProvider<?>>();

        Database database = getDatabase();

        singletons.add( new DatabaseProvider( database ) );
        singletons.add( new DatabaseActions.Provider( databaseActions ) );
        singletons.add( new GraphDatabaseServiceProvider( database ) );
        singletons.add( new NeoServerProvider( this ) );
        singletons.add( new ConfigurationProvider( getConfiguration() ) );

        singletons.add( new RrdDbProvider( database ) );

        singletons.add( new WebServerProvider( getWebServer() ) );

        PluginInvocatorProvider pluginInvocatorProvider = new PluginInvocatorProvider( this );
        singletons.add( pluginInvocatorProvider );
        RepresentationFormatRepository repository = new RepresentationFormatRepository( this );

        singletons.add( new InputFormatProvider( repository ) );
        singletons.add( new OutputFormatProvider( repository ) );
        singletons.add( new CypherExecutorProvider( cypherExecutor ) );
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
}
