/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import static java.lang.String.format;
import static org.neo4j.configuration.ssl.SslPolicyScope.HTTPS;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;

import io.netty.channel.local.LocalAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.ws.rs.ext.MessageBodyWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.jetty.io.ArrayByteBufferPool;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.bind.ComponentsBinder;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.http.HttpMemoryPool;
import org.neo4j.server.http.HttpTransactionMemoryPool;
import org.neo4j.server.http.cypher.HttpTransactionManager;
import org.neo4j.server.http.cypher.TransactionRegistry;
import org.neo4j.server.httpv2.driver.LocalChannelDriverFactory;
import org.neo4j.server.httpv2.metrics.QueryAPIMetricsMonitor;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.repr.RepresentationBasedMessageBodyWriter;
import org.neo4j.server.web.RotatingRequestLog;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public abstract class AbstractNeoWebServer extends LifecycleAdapter implements NeoWebServer {
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     * <p>
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    static final String NEO4J_IS_STARTING_MESSAGE = "======== Neo4j " + Version.getNeo4jVersion() + " ========";

    protected final InternalLogProvider userLogProvider;
    private final InternalLog log;
    private final DbmsInfo dbmsInfo;
    private final MemoryPool requestMemoryPool;
    private final MemoryPool transactionMemoryPool;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final List<Pattern> authWhitelist;
    private final DatabaseManagementService databaseManagementService;
    private final Dependencies globalDependencies;
    private final Config config;
    private final TransactionManager transactionManager;
    private final LifeSupport life = new LifeSupport();
    private final boolean httpEnabled;
    private final boolean httpsEnabled;
    private SocketAddress httpListenAddress;
    private SocketAddress httpsListenAddress;
    private SocketAddress httpAdvertisedAddress;
    private SocketAddress httpsAdvertisedAddress;

    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    protected ArrayByteBufferPool byteBufferPool;
    private final Supplier<SslPolicyLoader> sslPolicyFactorySupplier;
    private final HttpTransactionManager httpTransactionManager;

    private volatile Driver driver;
    protected final QueryAPIMetricsMonitor metricsMonitor;

    private final CompositeDatabaseAvailabilityGuard globalAvailabilityGuard;
    protected final SystemNanoClock clock;

    protected ConnectorPortRegister connectorPortRegister;
    private RotatingRequestLog requestLog;

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    public AbstractNeoWebServer(
            DatabaseManagementService databaseManagementService,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo,
            MemoryPools memoryPools,
            TransactionManager transactionManager,
            Monitors monitors,
            SystemNanoClock clock) {
        this.databaseManagementService = databaseManagementService;
        this.globalDependencies = globalDependencies;
        this.transactionManager = transactionManager;
        this.config = config;
        this.userLogProvider = userLogProvider;
        this.log = userLogProvider.getLog(getClass());
        this.dbmsInfo = dbmsInfo;
        this.metricsMonitor = monitors.newMonitor(QueryAPIMetricsMonitor.class);
        this.clock = clock;
        log.info(NEO4J_IS_STARTING_MESSAGE);

        byteBufferPool = new ArrayByteBufferPool();
        requestMemoryPool = new HttpMemoryPool(memoryPools, byteBufferPool);
        life.add(new MemoryPoolLifecycleAdapter(requestMemoryPool));

        transactionMemoryPool = new HttpTransactionMemoryPool(memoryPools);
        life.add(new MemoryPoolLifecycleAdapter(transactionMemoryPool));

        verifyConnectorsConfiguration(config);

        httpEnabled = config.get(HttpConnector.enabled);
        if (httpEnabled) {
            httpListenAddress = config.get(HttpConnector.listen_address);
            httpAdvertisedAddress = config.get(HttpConnector.advertised_address);
        }

        httpsEnabled = config.get(HttpsConnector.enabled);
        if (httpsEnabled) {
            httpsListenAddress = config.get(HttpsConnector.listen_address);
            httpsAdvertisedAddress = config.get(HttpsConnector.advertised_address);
        }

        this.authWhitelist = parseAuthWhitelist(config);
        authManagerSupplier = globalDependencies.provideDependency(AuthManager.class);
        sslPolicyFactorySupplier = globalDependencies.provideDependency(SslPolicyLoader.class);
        connectorPortRegister = globalDependencies.resolveDependency(ConnectorPortRegister.class);
        httpTransactionManager = createHttpTransactionManager();
        globalAvailabilityGuard = globalDependencies.resolveDependency(CompositeDatabaseAvailabilityGuard.class);

        life.add(new ServerComponentsLifecycleAdapter());
    }

    private Driver getOrCreateDriver() {

        var availableDriver = this.driver;
        if (availableDriver == null) {
            synchronized (this) {
                availableDriver = this.driver;
                if (availableDriver == null) {
                    var internalLogProvider = globalDependencies
                            .resolveDependency(LogService.class)
                            .getInternalLogProvider();
                    var driverFactory = new LocalChannelDriverFactory(
                            new LocalAddress(config.get(BoltConnectorInternalSettings.local_channel_address)),
                            internalLogProvider);
                    this.driver = driverFactory.createLocalDriver();
                    availableDriver = this.driver;
                }
            }
        }
        return availableDriver;
    }

    protected Dependencies getGlobalDependencies() {
        return globalDependencies;
    }

    @Override
    public DbmsInfo getDbmsInfo() {
        return dbmsInfo;
    }

    @Override
    public void init() {
        life.init();
    }

    @Override
    public void start() throws ServerStartupException {
        try {
            life.start();
        } catch (Throwable t) {
            // If the database has been started, attempt to cleanly shut it down to avoid unclean shutdowns.
            life.shutdown();
            var rootCause = ExceptionUtils.getRootCause(t);
            throw new ServerStartupException(format("Starting Neo4j failed: %s", rootCause.getMessage()), rootCause);
        }
    }

    private HttpTransactionManager createHttpTransactionManager() {
        JobScheduler jobScheduler = globalDependencies.resolveDependency(JobScheduler.class);
        Clock clock = Clocks.systemClock();
        Duration transactionTimeout = getTransactionTimeout();
        var routingEnabled = config.get(GraphDatabaseSettings.routing_enabled);
        return new HttpTransactionManager(
                databaseManagementService,
                transactionMemoryPool,
                jobScheduler,
                clock,
                transactionTimeout,
                userLogProvider,
                transactionManager,
                authManagerSupplier.get(),
                routingEnabled);
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private Duration getTransactionTimeout() {
        final long timeout = config.get(ServerSettings.http_transaction_timeout).toMillis();
        return Duration.ofMillis(Math.max(timeout, MINIMUM_TIMEOUT + ROUNDING_SECOND));
    }

    /**
     * Use this method to register server modules from subclasses
     */
    private void registerModule(ServerModule module) {
        serverModules.add(module);
    }

    private void startModules() {
        for (ServerModule module : serverModules) {
            module.start();
        }
    }

    private void stopModules() {
        final List<Exception> errors = new ArrayList<>();
        for (final ServerModule module : serverModules) {
            try {
                module.stop();
            } catch (Exception e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            final RuntimeException e = new RuntimeException();
            errors.forEach(e::addSuppressed);
            throw e;
        }
    }

    private void clearModules() {
        serverModules.clear();
    }

    @Override
    public Config getConfig() {
        return config;
    }

    protected void configureWebServer() {
        webServer.setHttpAddress(httpListenAddress);
        webServer.setHttpsAddress(httpsListenAddress);
        webServer.setMaxThreads(config.get(ServerSettings.webserver_max_threads));
        webServer.setWadlEnabled(config.get(ServerSettings.wadl_enabled));
        webServer.setComponentsBinder(createComponentsBinder());

        if (httpsEnabled) // only load sslPolicy when encryption is enabled
        {
            SslPolicyLoader sslPolicyLoader = sslPolicyFactorySupplier.get();
            if (sslPolicyLoader.hasPolicyForSource(HTTPS)) {
                webServer.setSslPolicy(sslPolicyLoader.getPolicy(HTTPS));
            }
        }
    }

    protected void startWebServer() throws Exception {
        try {
            setUpHttpLogging();
            webServer.start();
            registerHttpAddressAfterStartup();
            registerHttpsAddressAfterStartup();
            var protocol = httpsListenAddress != null ? "HTTPS" : "HTTP";
            var url = httpsListenAddress != null ? httpsListenAddress : httpListenAddress;
            log.info("%s enabled on %s.", protocol, url);
            log.info("Remote interface available at %s", getBaseUri());
        } catch (Exception e) {
            SocketAddress address = httpListenAddress != null ? httpListenAddress : httpsListenAddress;
            log.error("Failed to start Neo4j on %s: %s", address, e.getMessage());
            throw e;
        }
    }

    private void registerHttpAddressAfterStartup() {
        if (httpEnabled) {
            InetSocketAddress localHttpAddress = webServer.getLocalHttpAddress();
            connectorPortRegister.register(ConnectorType.HTTP, localHttpAddress);
            if (httpAdvertisedAddress.getPort() == 0) {
                httpAdvertisedAddress = new SocketAddress(localHttpAddress.getHostString(), localHttpAddress.getPort());
            }
        }
    }

    private void registerHttpsAddressAfterStartup() {
        if (httpsEnabled) {
            InetSocketAddress localHttpsAddress = webServer.getLocalHttpsAddress();
            connectorPortRegister.register(ConnectorType.HTTPS, localHttpsAddress);
            if (httpsAdvertisedAddress.getPort() == 0) {
                httpsAdvertisedAddress =
                        new SocketAddress(localHttpsAddress.getHostString(), localHttpsAddress.getPort());
            }
        }
    }

    private void setUpHttpLogging() {
        if (!getConfig().get(http_logging_enabled)) {
            return;
        }

        LogService logService = globalDependencies.resolveDependency(LogService.class);
        requestLog = new RotatingRequestLog(logService.getInternalLogProvider());
        webServer.setRequestLog(requestLog);
    }

    protected List<Pattern> getUriWhitelist() {
        return authWhitelist;
    }

    @Override
    public void stop() {
        shutdownGlobalAvailabilityGuard();
        life.stop();
    }

    private void shutdownGlobalAvailabilityGuard() {
        try {
            // Although the globalGuard availability guard is shutdown as part of LifeSupport#stop(), we never hit that
            // if we're
            // blocking in LifeSupport#start() and the blocked starting components may be using this guard as a bail out
            // signal
            if (globalAvailabilityGuard != null) {
                globalAvailabilityGuard.stop();
            }
        } catch (Throwable t) {
            // Not much we can do other than log - we're trying to shutdown anyway
            log.error(
                    "Failed to set the global availability guard to shutdown in the process of stopping the Neo4j server",
                    t);
        }
    }

    private void stopWebServer() throws Exception {
        if (webServer != null) {
            webServer.stop();
        }
        if (requestLog != null) {
            requestLog.stop();
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (driver != null) {
            driver.close();
            driver = null;
        }
    }

    @Override
    public TransactionRegistry getTransactionRegistry() {
        return httpTransactionManager.getTransactionRegistry();
    }

    @Override
    public URI getBaseUri() {
        return httpAdvertisedAddress != null
                ? SimpleUriBuilder.buildURI(httpAdvertisedAddress, false)
                : SimpleUriBuilder.buildURI(httpsAdvertisedAddress, true);
    }

    @Override
    public Optional<URI> httpsUri() {
        return Optional.ofNullable(httpsAdvertisedAddress).map(address -> SimpleUriBuilder.buildURI(address, true));
    }

    public WebServer getWebServer() {
        return webServer;
    }

    private ComponentsBinder createComponentsBinder() {
        ComponentsBinder binder = new ComponentsBinder();

        var databaseStateService = getGlobalDependencies().resolveDependency(DatabaseStateService.class);
        var databaseResolver = getGlobalDependencies().resolveDependency(DefaultDatabaseResolver.class);
        binder.addSingletonBinding(databaseManagementService, DatabaseManagementService.class);
        binder.addSingletonBinding(databaseStateService, DatabaseStateService.class);
        binder.addSingletonBinding(this, NeoWebServer.class);
        binder.addSingletonBinding(getConfig(), Config.class);
        binder.addSingletonBinding(transactionMemoryPool, MemoryPool.class);
        binder.addSingletonBinding(getWebServer(), WebServer.class);
        binder.bind(RepresentationBasedMessageBodyWriter.class).to(MessageBodyWriter.class);
        binder.addSingletonBinding(httpTransactionManager, HttpTransactionManager.class);
        binder.addSingletonBinding(databaseResolver, DefaultDatabaseResolver.class);
        binder.addLazyBinding(authManagerSupplier, AuthManager.class);
        binder.addSingletonBinding(userLogProvider, InternalLogProvider.class);
        binder.addSingletonBinding(userLogProvider.getLog(NeoWebServer.class), InternalLog.class);
        binder.addLazyBinding(this::getOrCreateDriver, Driver.class);

        return binder;
    }

    private static void verifyConnectorsConfiguration(Config config) {
        boolean httpAndHttpsDisabled = !config.get(HttpConnector.enabled) && !config.get(HttpsConnector.enabled);
        if (httpAndHttpsDisabled) {
            throw new IllegalArgumentException("Either HTTP or HTTPS connector must be configured to run the server");
        }
    }

    private static List<Pattern> parseAuthWhitelist(Config config) {
        return config.get(ServerSettings.http_auth_allowlist).stream()
                .map(Pattern::compile)
                .toList();
    }

    private class ServerComponentsLifecycleAdapter extends LifecycleAdapter {
        @Override
        public void init() {
            webServer = createWebServer();

            for (ServerModule moduleClass : createServerModules()) {
                registerModule(moduleClass);
            }
        }

        @Override
        public void start() throws Exception {
            LogService logService = globalDependencies.resolveDependency(LogService.class);
            InternalLog serverLog = logService.getInternalLog(ServerComponentsLifecycleAdapter.class);
            serverLog.info("Starting web server");
            configureWebServer();

            startModules();

            startWebServer();

            serverLog.info("Web server started.");
        }

        @Override
        public void stop() throws Exception {
            stopWebServer();
            stopModules();
            clearModules();
        }
    }

    private static class MemoryPoolLifecycleAdapter extends LifecycleAdapter {
        private final MemoryPool memoryPool;

        private MemoryPoolLifecycleAdapter(MemoryPool memoryPool) {
            this.memoryPool = memoryPool;
        }

        @Override
        public void shutdown() throws Exception {
            memoryPool.free();
        }
    }
}
