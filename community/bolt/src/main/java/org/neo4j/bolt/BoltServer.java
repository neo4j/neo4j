/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt;

import static org.neo4j.bolt.protocol.common.connection.DefaultConnectionHintProvider.connectionHintProviderFunction;
import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.net.ssl.SSLException;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.bookmark.BookmarkParser;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.ConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.AtomicSchedulingConnection;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.executor.ExecutorServiceFactory;
import org.neo4j.bolt.protocol.common.connector.executor.NettyThreadFactory;
import org.neo4j.bolt.protocol.common.connector.executor.ThreadPoolExecutorServiceFactory;
import org.neo4j.bolt.protocol.common.connector.listener.AuthenticationTimeoutConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.KeepAliveConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.MetricsConnectorListener;
import org.neo4j.bolt.protocol.common.connector.netty.DomainSocketNettyConnector;
import org.neo4j.bolt.protocol.common.connector.netty.SocketNettyConnector;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v40.bookmark.BookmarkParserV40;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v42.BoltProtocolV42;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v50.BoltProtocolV50;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.bolt.transaction.TransactionManager;
import org.neo4j.bolt.transport.BoltMemoryPool;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.buffer.CentralBufferMangerHolder;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SslSystemSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.CommonConnectorConfig;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.config.AuthConfigProvider;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

public class BoltServer extends LifecycleAdapter {

    @VisibleForTesting
    public static final PooledByteBufAllocator NETTY_BUF_ALLOCATOR =
            new PooledByteBufAllocator(PlatformDependent.directBufferPreferred());

    private DbmsInfo dbmsInfo;
    private final JobScheduler jobScheduler;
    private final ConnectorPortRegister connectorPortRegister;
    private final NetworkConnectionTracker connectionTracker;
    private final Config config;
    private final SystemNanoClock clock;
    private final Monitors monitors;
    private final LogService logService;
    private final AuthManager externalAuthManager;
    private final AuthManager internalAuthManager;
    private final AuthManager loopbackAuthManager;
    private final MemoryPools memoryPools;
    private final DefaultDatabaseResolver defaultDatabaseResolver;
    private final CentralBufferMangerHolder centralBufferMangerHolder;
    private final ConnectionHintProvider connectionHintProvider;

    private final ExecutorServiceFactory executorServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;
    private final BoltProtocolRegistry protocolRegistry;
    private final AuthConfigProvider authConfigProvider;
    private final InternalLog log;
    private final BookmarkParser bookmarkParser;

    private final InternalLog userLog;

    private final LifeSupport connectorLife = new LifeSupport();
    private BoltMemoryPool memoryPool;
    private EventLoopGroup eventLoopGroup;
    private ExecutorService executorService;
    private BoltConnectionMetricsMonitor metricsMonitor;

    public BoltServer(
            DbmsInfo dbmsInfo,
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            JobScheduler jobScheduler,
            ConnectorPortRegister connectorPortRegister,
            NetworkConnectionTracker connectionTracker,
            DatabaseIdRepository databaseIdRepository,
            Config config,
            SystemNanoClock clock,
            Monitors monitors,
            LogService logService,
            DependencyResolver dependencyResolver,
            AuthManager externalAuthManager,
            AuthManager internalAuthManager,
            AuthManager loopbackAuthManager,
            MemoryPools memoryPools,
            DefaultDatabaseResolver defaultDatabaseResolver,
            CentralBufferMangerHolder centralBufferMangerHolder,
            TransactionManager transactionManager) {
        this.dbmsInfo = dbmsInfo;
        this.jobScheduler = jobScheduler;
        this.connectorPortRegister = connectorPortRegister;
        this.connectionTracker = connectionTracker;
        this.config = config;
        this.clock = clock;
        this.monitors = monitors;
        this.logService = logService;
        this.externalAuthManager = externalAuthManager;
        this.internalAuthManager = internalAuthManager;
        this.loopbackAuthManager = loopbackAuthManager;
        this.memoryPools = memoryPools;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.centralBufferMangerHolder = centralBufferMangerHolder;
        this.connectionHintProvider = connectionHintProviderFunction.apply(config);

        this.executorServiceFactory = new ThreadPoolExecutorServiceFactory(
                config.get(BoltConnector.thread_pool_min_size),
                config.get(BoltConnector.thread_pool_max_size),
                true,
                config.get(BoltConnector.thread_pool_keep_alive),
                config.get(BoltConnectorInternalSettings.unsupported_thread_pool_queue_size),
                this.jobScheduler.threadFactory(Group.BOLT_WORKER));

        this.sslPolicyLoader = dependencyResolver.resolveDependency(SslPolicyLoader.class);
        this.authConfigProvider = dependencyResolver.resolveDependency(AuthConfigProvider.class);
        this.log = logService.getInternalLog(BoltServer.class);
        this.userLog = logService.getUserLog(BoltServer.class);

        var customBookmarkParser = boltGraphDatabaseManagementServiceSPI
                .getCustomBookmarkFormatParser()
                .orElse(CustomBookmarkFormatParser.DEFAULT);
        this.bookmarkParser = new BookmarkParserV40(databaseIdRepository, customBookmarkParser);

        this.protocolRegistry = BoltProtocolRegistry.builder()
                .register(new BoltProtocolV40(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV41(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV42(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV43(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV44(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .register(new BoltProtocolV50(
                        logService,
                        boltGraphDatabaseManagementServiceSPI,
                        defaultDatabaseResolver,
                        transactionManager,
                        clock))
                .build();
    }

    private boolean isEnabled() {
        return this.config.get(BoltConnector.enabled);
    }

    @Override
    public void init() {
        // TODO: Move to an earlier stage within the application initialization
        InternalLoggerFactory.setDefaultFactory(new Netty4LoggerFactory(logService.getInternalLogProvider()));

        this.memoryPool = new BoltMemoryPool(memoryPools, NETTY_BUF_ALLOCATOR.metric());

        if (config.get(CommonConnectorConfig.ocsp_stapling_enabled)) {
            enableOcspStapling();
            this.log.info("Enabled OCSP stapling support");
        }

        if (!this.isEnabled()) {
            return;
        }

        jobScheduler.setThreadFactory(Group.BOLT_NETWORK_IO, NettyThreadFactory::new);

        // permit all transport implementations so long as native transports have not been explicitly disabled in the
        // application configuration
        var permitNativeTransports = this.config.get(BoltConnectorInternalSettings.use_native_transport);
        Predicate<ConnectorTransport> filter;
        if (permitNativeTransports) {
            filter = transport -> true;
        } else {
            filter = Predicate.not(ConnectorTransport::isNative);
        }

        // select the most optimal transport according to its priority - should only throw in case of Class-Path issues
        // as we provide a NIO fallback
        var transport = ConnectorTransport.selectOptimal(filter)
                .orElseThrow(() ->
                        new IllegalStateException("No transport implementations available within current environment"));
        this.log.info("Using connector transport %s", transport.getName());

        this.eventLoopGroup = transport.createEventLoopGroup(this.jobScheduler.threadFactory(Group.BOLT_NETWORK_IO));
        this.executorService = this.executorServiceFactory.create();
        this.metricsMonitor = this.monitors.newMonitor(BoltConnectionMetricsMonitor.class);

        var connectionFactory = this.createConnectionFactory();

        if (config.get(BoltConnectorInternalSettings.enable_loopback_auth)) {
            this.registerConnector(this.createDomainSocketConnector(
                    connectionFactory, transport, createAuthentication(this.loopbackAuthManager)));

            this.log.info("Configured loopback (domain socket) Bolt connector");
        }

        var listenAddress = this.config.get(BoltConnector.listen_address).socketAddress();
        var level = this.config.get(BoltConnector.encryption_level);
        var encryptionRequired = level == EncryptionLevel.REQUIRED;

        SslContext sslContext = null;
        if (level != EncryptionLevel.DISABLED) {
            if (!this.sslPolicyLoader.hasPolicyForSource(BOLT)) {
                throw new IllegalStateException(
                        "Requested encryption level " + level + " for Bolt connector but no SSL policy was given");
            }

            try {
                sslContext = this.sslPolicyLoader.getPolicy(BOLT).nettyServerContext();
            } catch (SSLException ex) {
                throw new IllegalStateException("Failed to load SSL policy for Bolt connector", ex);
            }
        }

        this.registerConnector(this.createSocketConnector(
                listenAddress,
                connectionFactory,
                encryptionRequired,
                transport,
                sslContext,
                createAuthentication(externalAuthManager),
                ConnectorType.BOLT));

        this.log.info("Configured external Bolt connector with listener address %s", listenAddress);

        var isRoutingEnabled = config.get(GraphDatabaseSettings.routing_enabled);
        if (isRoutingEnabled && dbmsInfo == DbmsInfo.ENTERPRISE) {
            SocketAddress internalListenAddress;
            if (this.config.isExplicitlySet(GraphDatabaseSettings.routing_listen_address)) {
                internalListenAddress =
                        config.get(GraphDatabaseSettings.routing_listen_address).socketAddress();
            } else {
                internalListenAddress = new InetSocketAddress(
                        this.config.get(BoltConnector.listen_address).getHostname(),
                        this.config
                                .get(GraphDatabaseSettings.routing_listen_address)
                                .getPort());
            }

            var internalEncryptionRequired = false;
            SslContext internalSslContext = null;

            if (this.sslPolicyLoader.hasPolicyForSource(CLUSTER)) {
                internalEncryptionRequired = true;

                try {
                    internalSslContext = this.sslPolicyLoader.getPolicy(CLUSTER).nettyServerContext();
                } catch (SSLException ex) {
                    throw new IllegalStateException(
                            "Failed to load SSL policy for server side routing within Bolt: Cluster policy", ex);
                }
            }

            this.registerConnector(this.createSocketConnector(
                    internalListenAddress,
                    connectionFactory,
                    internalEncryptionRequired,
                    transport,
                    internalSslContext,
                    createAuthentication(internalAuthManager),
                    ConnectorType.INTRA_BOLT));

            this.log.info("Configured internal Bolt connector with listener address %s", internalListenAddress);
        }

        this.log.info("Bolt server loaded");
        this.connectorLife.init();
    }

    @Override
    public void start() throws Exception {
        if (!this.isEnabled()) {
            return;
        }

        this.connectorLife.start();
        this.log.info("Bolt server started");
    }

    @Override
    public void stop() throws Exception {
        if (!this.isEnabled()) {
            return;
        }

        log.info("Requested Bolt server shutdown");
        this.connectorLife.stop();
    }

    @Override
    public void shutdown() {
        if (this.isEnabled()) {
            log.info("Shutting down Bolt server");

            // send shutdown notifications to all of our connectors in order to perform the necessary shutdown
            // procedures for the remaining connections
            this.connectorLife.shutdown();

            // once the remaining connections have been shut down, we'll request a graceful shutdown from the network
            // thread pool
            // TODO: Force shutdown if timeout exceeded?
            this.eventLoopGroup.shutdownGracefully().awaitUninterruptibly();
            this.eventLoopGroup
                    .shutdownGracefully(
                            this.config.get(GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period),
                            this.config
                                    .get(GraphDatabaseInternalSettings.netty_server_shutdown_timeout)
                                    .toSeconds(),
                            TimeUnit.SECONDS)
                    .syncUninterruptibly();

            // also make sure that our executor service is cleanly shut down - there should be no remaining jobs present
            // as connectors will kill any remaining jobs forcefully as part of their shutdown procedures
            var remainingJobs = this.executorService.shutdownNow();
            if (!remainingJobs.isEmpty()) {
                log.warn("Forcefully killed %d remaining Bolt jobs to fulfill shutdown request", remainingJobs.size());
            }

            log.info("Bolt server has been shut down");
        }

        this.memoryPool.close();
    }

    private ByteBufAllocator getBufferAllocator() {
        // check if there is a Netty buffer allocator managed centrally
        // such allocator has also memory management done centrally
        if (centralBufferMangerHolder.getNettyBufferAllocator() != null) {
            return centralBufferMangerHolder.getNettyBufferAllocator();
        }

        connectorLife.add(new BoltMemoryPoolLifeCycleAdapter(memoryPool));
        return NETTY_BUF_ALLOCATOR;
    }

    private void registerConnector(Connector connector) {
        // append a listener which handles the creation of metrics
        connector.registerListener(new MetricsConnectorListener(this.metricsMonitor));

        // if an authentication timeout has been configured, we'll register a listener which appends the necessary
        // timeout handlers with the network pipelines upon connection creation
        var authenticationTimeout =
                this.config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout);
        if (!authenticationTimeout.isZero()) {
            connector.registerListener(new AuthenticationTimeoutConnectorListener(
                    authenticationTimeout, this.logService.getInternalLogProvider()));
        }

        // if keep-alive have been configured, we'll register a listener which appends the necessary handlers to the
        // network pipelines upon connection negotiation
        var keepAliveMechanism = config.get(BoltConnector.connection_keep_alive_type);
        var keepAliveInterval = config.get(BoltConnector.connection_keep_alive).toMillis();
        if (keepAliveMechanism != BoltConnector.KeepAliveRequestType.OFF) {
            connector.registerListener(new KeepAliveConnectorListener(
                    keepAliveMechanism != BoltConnector.KeepAliveRequestType.ALL,
                    keepAliveInterval,
                    this.logService.getInternalLogProvider()));
        }

        this.connectorLife.add(connector);
    }

    private Connection.Factory createConnectionFactory() {
        return new AtomicSchedulingConnection.Factory(this.executorService, this.clock, this.logService);
    }

    private static Authentication createAuthentication(AuthManager authManager) {
        return new BasicAuthentication(authManager);
    }

    private void enableOcspStapling() {
        if (SslProvider.JDK.equals(this.config.get(SslSystemSettings.netty_ssl_provider))) {
            // currently the only way to enable OCSP server stapling for JDK is through this property
            System.setProperty("jdk.tls.server.enableStatusRequestExtension", "true");
        } else {
            throw new IllegalArgumentException("OCSP Server stapling can only be used with JDK ssl provider (see "
                    + SslSystemSettings.netty_ssl_provider.name() + ")");
        }
    }

    private Connector createSocketConnector(
            SocketAddress bindAddress,
            Connection.Factory connectionFactory,
            boolean encryptionRequired,
            ConnectorTransport transport,
            SslContext sslContext,
            Authentication authentication,
            ConnectorType connectorType) {
        return new SocketNettyConnector(
                BoltConnector.NAME,
                bindAddress,
                this.config,
                connectorType,
                this.connectorPortRegister,
                this.memoryPool,
                this.getBufferAllocator(),
                this.eventLoopGroup,
                transport,
                connectionFactory,
                this.connectionTracker,
                sslContext,
                encryptionRequired,
                config.get(BoltConnectorInternalSettings.tcp_keep_alive),
                this.protocolRegistry,
                authentication,
                this.authConfigProvider,
                this.defaultDatabaseResolver,
                this.connectionHintProvider,
                this.bookmarkParser,
                this.logService.getUserLogProvider(),
                this.logService.getInternalLogProvider());
    }

    private Connector createDomainSocketConnector(
            Connection.Factory connectionFactory, ConnectorTransport transport, Authentication authentication) {
        if (this.config.get(BoltConnectorInternalSettings.unsupported_loopback_listen_file) == null) {
            throw new IllegalArgumentException(
                    "A file has not been specified for use with the loopback domain socket.");
        }

        var file = new File(config.get(BoltConnectorInternalSettings.unsupported_loopback_listen_file)
                .toString());

        return new DomainSocketNettyConnector(
                BoltConnectorInternalSettings.LOOPBACK_NAME,
                file,
                this.config,
                this.memoryPool,
                this.getBufferAllocator(),
                this.eventLoopGroup,
                transport,
                connectionFactory,
                this.connectionTracker,
                this.protocolRegistry,
                authentication,
                this.authConfigProvider,
                this.defaultDatabaseResolver,
                this.connectionHintProvider,
                this.bookmarkParser,
                this.logService.getUserLogProvider(),
                this.logService.getInternalLogProvider());
    }

    private static class BoltMemoryPoolLifeCycleAdapter extends LifecycleAdapter {
        private final BoltMemoryPool pool;

        private BoltMemoryPoolLifeCycleAdapter(BoltMemoryPool pool) {
            this.pool = pool;
        }

        @Override
        public void shutdown() {
            pool.close();
        }
    }
}
