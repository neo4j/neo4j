/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.server.configuration.Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.GraphDatabaseFactory;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.modules.PluginInitializer;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.rest.security.SecurityRule;
import org.neo4j.server.security.KeyStoreFactory;
import org.neo4j.server.security.KeyStoreInformation;
import org.neo4j.server.security.SslCertificateFactory;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;

public class NeoServerWithEmbeddedWebServer implements NeoServer
{
    public static final Logger log = Logger.getLogger( NeoServerWithEmbeddedWebServer.class );

    private Database database;
    private final Configurator configurator;
    private final WebServer webServer;
    private final StartupHealthCheck startupHealthCheck;

    private final List<ServerModule> serverModules = new ArrayList<ServerModule>();
    private PluginInitializer pluginInitializer;
    private final Bootstrapper bootstrapper;

    private SimpleUriBuilder uriBuilder = new SimpleUriBuilder();

    public NeoServerWithEmbeddedWebServer( Bootstrapper bootstrapper,
                                           StartupHealthCheck startupHealthCheck, Configurator configurator, WebServer webServer,
                                           Iterable<Class<? extends ServerModule>> moduleClasses )
    {

        this.bootstrapper = bootstrapper;
        this.startupHealthCheck = startupHealthCheck;
        this.configurator = configurator;
        this.webServer = webServer;

        webServer.setNeoServer( this );
        for ( Class<? extends ServerModule> moduleClass : moduleClasses )
        {
            registerModule( moduleClass );
        }
    }

    @Override
    public void start()
    {
        // Start at the bottom of the stack and work upwards to the Web
        // container
        startupHealthCheck();

        initWebServer();

        DiagnosticsManager diagnosticsManager = startDatabase();

        StringLogger logger = diagnosticsManager.getTargetLog();
        logger.logMessage( "--- SERVER STARTUP START ---" );

        diagnosticsManager.register( Configurator.DIAGNOSTICS, configurator );

        startExtensionInitialization();

        startModules( logger );

        startWebServer( logger );

        logger.logMessage( "--- SERVER STARTUP END ---", true );
    }

    /**
     * Initializes individual plugins using the mechanism provided via {@link PluginInitializer} and the java service
     * locator
     */
    protected void startExtensionInitialization()
    {
        pluginInitializer = new PluginInitializer( this );
    }

    /**
     * Use this method to register server modules from subclasses
     *
     * @param clazz
     */
    protected final void registerModule( Class<? extends ServerModule> clazz )
    {
        try
        {
            serverModules.add( clazz.newInstance() );
        }
        catch ( Exception e )
        {
            log.warn( "Failed to instantiate server module [%s], reason: %s", clazz.getName(), e.getMessage() );
        }
    }

    private void startModules( StringLogger logger )
    {
        for ( ServerModule module : serverModules )
        {
            module.start( this, logger );
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

    private void startupHealthCheck()
    {
        if ( !startupHealthCheck.run() )
        {
            throw new StartupHealthCheckFailedException( startupHealthCheck.failedRule() );
        }
    }

    private DiagnosticsManager startDatabase()
    {
        String dbLocation = new File( configurator.configuration()
            .getString(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY ) ).getAbsolutePath();
        GraphDatabaseFactory dbFactory = bootstrapper.getGraphDatabaseFactory( configurator.configuration() );

        Map<String, String> databaseTuningProperties = configurator.getDatabaseTuningProperties();
        if ( databaseTuningProperties != null )
        {
            this.database = new Database( dbFactory, dbLocation, databaseTuningProperties );
        }
        else
        {
            this.database = new Database( dbFactory, dbLocation );
        }
        return database.graph.getDiagnosticsManager();
    }

    @Override
    public Configuration getConfiguration()
    {
        return configurator.configuration();
    }

    private void initWebServer()
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
        if ( sslEnabled )
        {
            log.info( "Enabling HTTPS on port [%s]", sslPort );
            webServer.setHttpsCertificateInformation( initHttpsKeyStore() );
        }

        webServer.init();
    }

    private SecurityRule[] createSecurityRulesFrom( Configuration configuration )
    {
        ArrayList<SecurityRule> rules = new ArrayList<SecurityRule>();

        for ( String classname : configuration.getStringArray( Configurator.SECURITY_RULES_KEY ) )
        {
            try
            {
                rules.add( (SecurityRule) Class.forName( classname ).newInstance() );
            }
            catch ( Exception e )
            {
                log.error( "Could not load server security rule [%s], exception details: ", classname, e.getMessage() );
                e.printStackTrace();
            }
        }

        return rules.toArray( new SecurityRule[0] );
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
            SecurityRule[] securityRules = createSecurityRulesFrom( configurator.configuration() );
            webServer.addSecurityRules( securityRules );

            Integer limit = getConfiguration().getInteger( WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, null );
            if ( limit != null )
            {
                webServer.addExecutionLimitFilter( limit );
            }


            if ( httpLoggingProperlyConfigured() )
            {
                webServer.enableHTTPLoggingForWebadmin(
                    new File( getConfiguration().getProperty( Configurator.HTTP_LOG_CONFIG_LOCATION ).toString() ) );
            }

            webServer.start();

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
        if ( property == null )
        {
            return false;
        }

        return new File( String.valueOf( property ) ).exists();
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
     */
    public void stopServerOnly()
    {
        try
        {
            stopWebServer();
            stopModules();
            stopExtensionInitializers();
            log.info( "Successfully shutdown Neo4j Server." );
        }
        catch ( Exception e )
        {
            log.warn( "Failed to cleanly shutdown Neo4j Server." );
        }
    }

    /**
     * shuts down initializers of individual plugins
     */
    private void stopExtensionInitializers()
    {
        pluginInitializer.stop();
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
            database.shutdown();
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

    @Override
    public Collection<Injectable<?>> getInjectables( List<String> packageNames )
    {
        return pluginInitializer.initializePackages( packageNames );
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
