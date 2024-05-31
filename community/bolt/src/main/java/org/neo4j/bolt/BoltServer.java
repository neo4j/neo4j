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
package org.neo4j.bolt;

import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.function.Suppliers.lazySingleton;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.PlatformDependent;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.net.ssl.SSLException;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.protocol.common.connection.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.BoltDriverMetricsMonitor;
import org.neo4j.bolt.protocol.common.connection.hint.ConnectionHintRegistry;
import org.neo4j.bolt.protocol.common.connection.hint.KeepAliveConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connection.hint.TelemetryConnectionHintProvider;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.accounting.error.CircuitBreakerErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.error.ErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.error.NoopErrorAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.AtomicTrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.NoopTrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.accounting.traffic.TrafficAccountant;
import org.neo4j.bolt.protocol.common.connector.connection.AtomicSchedulingConnection;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.executor.ExecutorServiceFactory;
import org.neo4j.bolt.protocol.common.connector.executor.NettyThreadFactory;
import org.neo4j.bolt.protocol.common.connector.executor.ThreadPoolExecutorServiceFactory;
import org.neo4j.bolt.protocol.common.connector.listener.AuthenticationTimeoutConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.KeepAliveConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.MetricsConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.ReadLimitConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.ResetMessageConnectorListener;
import org.neo4j.bolt.protocol.common.connector.listener.ResponseMetricsConnectorListener;
import org.neo4j.bolt.protocol.common.connector.netty.DomainSocketNettyConnector;
import org.neo4j.bolt.protocol.common.connector.netty.LocalNettyConnector;
import org.neo4j.bolt.protocol.common.connector.netty.LocalNettyConnector.LocalConfiguration;
import org.neo4j.bolt.protocol.common.connector.netty.SocketNettyConnector;
import org.neo4j.bolt.protocol.common.connector.transport.ConnectorTransport;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.bolt.transport.BoltMemoryPool;
import org.neo4j.bolt.transport.NettyMemoryPool;
import org.neo4j.bolt.tx.TransactionManager;
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
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
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
    public static final Suppliers.Lazy<PooledByteBufAllocator> NETTY_BUF_ALLOCATOR =
            lazySingleton(() -> new PooledByteBufAllocator(PlatformDependent.directBufferPreferred()));

    private final DbmsInfo dbmsInfo;
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
    private final ConnectionHintRegistry connectionHintRegistry;

    private final ExecutorServiceFactory executorServiceFactory;
    private final SslPolicyLoader sslPolicyLoader;
    private final BoltProtocolRegistry protocolRegistry;
    private final AuthConfigProvider authConfigProvider;
    private final TransactionManager transactionManager;
    private final RoutingService routingService;
    private final InternalLog log;

    private final LifeSupport connectorLife = new LifeSupport();
    private BoltMemoryPool memoryPool;
    private EventLoopGroup bossEventLoopGroup;
    private EventLoopGroup workerEventLoopGroup;
    private ExecutorService executorService;
    private BoltConnectionMetricsMonitor connectionMetricsMonitor;
    private BoltDriverMetricsMonitor driverMetricsMonitor;

    public BoltServer(
            DbmsInfo dbmsInfo,
            JobScheduler jobScheduler,
            ConnectorPortRegister connectorPortRegister,
            NetworkConnectionTracker connectionTracker,
            TransactionManager transactionManager,
            Config config,
            SystemNanoClock clock,
            Monitors monitors,
            LogService logService,
            DependencyResolver dependencyResolver,
            AuthManager externalAuthManager,
            AuthManager internalAuthManager,
            AuthManager loopbackAuthManager,
            MemoryPools memoryPools,
            RoutingService routingService,
            DefaultDatabaseResolver defaultDatabaseResolver) {
        this.dbmsInfo = dbmsInfo;
        this.jobScheduler = jobScheduler;
        this.connectorPortRegister = connectorPortRegister;
        this.connectionTracker = connectionTracker;
        this.transactionManager = transactionManager;
        this.config = config;
        this.clock = clock;
        this.monitors = monitors;
        this.logService = logService;
        this.externalAuthManager = externalAuthManager;
        this.internalAuthManager = internalAuthManager;
        this.loopbackAuthManager = loopbackAuthManager;
        this.memoryPools = memoryPools;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
        this.connectionHintRegistry = ConnectionHintRegistry.newBuilder()
                .withProvider(new KeepAliveConnectionHintProvider(config))
                .withProvider(new TelemetryConnectionHintProvider(config))
                .build();

        this.executorServiceFactory = new ThreadPoolExecutorServiceFactory(
                config.get(BoltConnector.thread_pool_min_size),
                config.get(BoltConnector.thread_pool_max_size),
                true,
                config.get(BoltConnector.thread_pool_keep_alive),
                config.get(BoltConnectorInternalSettings.unsupported_thread_pool_queue_size),
                this.jobScheduler.threadFactory(Group.BOLT_WORKER));

        this.routingService = routingService;

        this.sslPolicyLoader = dependencyResolver.resolveDependency(SslPolicyLoader.class);
        this.authConfigProvider = dependencyResolver.resolveDependency(AuthConfigProvider.class);
        this.log = logService.getInternalLog(BoltServer.class);

        this.protocolRegistry = BoltProtocolRegistry.builder()
                .register(BoltProtocol.available())
                .build();
    }

    private boolean isEnabled() {
        return config.get(BoltConnector.enabled);
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public void init() {
        if (!isEnabled()) {
            return;
        }

        if (config.get(CommonConnectorConfig.ocsp_stapling_enabled)) {
            enableOcspStapling();
            log.info("Enabled OCSP stapling support");
        }

        jobScheduler.setThreadFactory(Group.BOLT_NETWORK_IO, NettyThreadFactory::new);

        Predicate<ConnectorTransport> filter;
        if (config.get(BoltConnectorInternalSettings.use_native_transport)) {
            // permit all transport implementations so long as native transports have not been explicitly disabled in
            // the application configuration
            filter = transport -> true;
        } else {
            filter = Predicate.not(ConnectorTransport::isNative);
        }

        // select the most optimal transport according to its priority - should only throw in case of Class-Path issues
        // as we provide a NIO fallback
        var transport = ConnectorTransport.selectOptimal(filter)
                .orElseThrow(() ->
                        new IllegalStateException("No transport implementations available within current environment"));
        log.info("Using connector transport %s", transport.getName());

        bossEventLoopGroup = transport.createEventLoopGroup(jobScheduler.threadFactory(Group.BOLT_NETWORK_IO));
        workerEventLoopGroup = transport.createEventLoopGroup(jobScheduler.threadFactory(Group.BOLT_NETWORK_IO));
        executorService = executorServiceFactory.create();
        connectionMetricsMonitor = monitors.newMonitor(BoltConnectionMetricsMonitor.class);

        if (config.get(BoltConnector.server_bolt_telemetry_enabled)) {
            driverMetricsMonitor = monitors.newMonitor(BoltDriverMetricsMonitor.class);
        } else {
            driverMetricsMonitor = BoltDriverMetricsMonitor.noop();
        }

        ByteBufAllocator allocator = getBufferAllocator();
        var connectionFactory = createConnectionFactory();

        var streamingBufferSize = config.get(BoltConnectorInternalSettings.streaming_buffer_size);
        var streamingFlushThreshold = config.get(BoltConnectorInternalSettings.streaming_flush_threshold);

        if (config.get(BoltConnectorInternalSettings.enable_loopback_auth)) {
            registerConnector(createDomainSocketConnector(
                    connectionFactory, transport, createAuthentication(loopbackAuthManager), allocator));

            log.info("Configured loopback (domain socket) Bolt connector");
        }

        var listenAddress = config.get(BoltConnector.listen_address).socketAddress();
        var encryptionLevel = config.get(BoltConnector.encryption_level);
        boolean encryptionRequired = encryptionLevel == EncryptionLevel.REQUIRED;

        SslContext sslContext = null;
        if (encryptionLevel != EncryptionLevel.DISABLED) {
            if (!sslPolicyLoader.hasPolicyForSource(BOLT)) {
                throw new IllegalStateException("Requested encryption level " + encryptionLevel
                        + " for Bolt connector but no SSL policy was given");
            }

            try {
                sslContext = sslPolicyLoader.getPolicy(BOLT).nettyServerContext();
            } catch (SSLException ex) {
                throw new IllegalStateException("Failed to load SSL policy for Bolt connector", ex);
            }
        }

        registerConnector(createSocketConnector(
                listenAddress,
                connectionFactory,
                encryptionRequired,
                transport,
                sslContext,
                createAuthentication(externalAuthManager),
                ConnectorType.BOLT,
                allocator));

        log.info("Configured external Bolt connector with listener address %s", listenAddress);

        boolean isRoutingEnabled = config.get(GraphDatabaseSettings.routing_enabled);
        if (isRoutingEnabled && dbmsInfo == DbmsInfo.ENTERPRISE) {
            SocketAddress internalListenAddress;
            if (config.isExplicitlySet(GraphDatabaseSettings.routing_listen_address)) {
                internalListenAddress =
                        config.get(GraphDatabaseSettings.routing_listen_address).socketAddress();
            } else {
                internalListenAddress = new InetSocketAddress(
                        config.get(BoltConnector.listen_address).getHostname(),
                        config.get(GraphDatabaseSettings.routing_listen_address).getPort());
            }

            var internalEncryptionRequired = false;
            SslContext internalSslContext = null;

            if (sslPolicyLoader.hasPolicyForSource(CLUSTER)) {
                internalEncryptionRequired = true;

                try {
                    internalSslContext = sslPolicyLoader.getPolicy(CLUSTER).nettyServerContext();
                } catch (SSLException ex) {
                    throw new IllegalStateException(
                            "Failed to load SSL policy for server side routing within Bolt: Cluster policy", ex);
                }
            }

            registerConnector(createSocketConnector(
                    internalListenAddress,
                    connectionFactory,
                    internalEncryptionRequired,
                    transport,
                    internalSslContext,
                    createAuthentication(internalAuthManager),
                    ConnectorType.INTRA_BOLT,
                    allocator));

            log.info("Configured internal Bolt connector with listener address %s", internalListenAddress);
        }

        if (config.get(BoltConnectorInternalSettings.enable_local_connector)) {
            registerConnector(createLocalConnector(
                    connectionFactory,
                    transport,
                    createAuthentication(externalAuthManager),
                    allocator,
                    streamingBufferSize,
                    streamingFlushThreshold));
        }

        log.info("Bolt server loaded");
        connectorLife.init();
    }

    @Override
    public void start() throws Exception {
        if (!isEnabled()) {
            return;
        }

        connectorLife.start();
        log.info("Bolt server started");
    }

    @Override
    public void stop() throws Exception {
        if (!isEnabled()) {
            return;
        }

        log.info("Requested Bolt server shutdown");
        connectorLife.stop();
    }

    @Override
    public void shutdown() {
        if (isEnabled()) {
            log.info("Shutting down Bolt server");

            // shutdown all accept threads prior to connection termination in order to prevent new
            // connections from being established to the server
            var bossTerminationFuture = bossEventLoopGroup.shutdownGracefully(
                    config.get(GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period),
                    config.get(GraphDatabaseInternalSettings.netty_server_shutdown_timeout)
                            .toSeconds(),
                    TimeUnit.SECONDS);

            var bossTerminationCompleted = bossTerminationFuture.awaitUninterruptibly(
                    config.get(BoltConnectorInternalSettings.thread_pool_shutdown_wait_time)
                            .toSeconds(),
                    TimeUnit.SECONDS);
            if (!bossTerminationCompleted) {
                log.warn(
                        "Termination of boss event loop group has exceeded maximum permitted duration - Remaining jobs will be forcefully terminated");
            } else if (!bossTerminationFuture.isSuccess()) {
                log.warn("Termination of boss event loop group has failed", bossTerminationFuture.cause());
            }

            // send shutdown notifications to all of our connectors in order to perform the necessary shutdown
            // procedures for the remaining connections
            connectorLife.shutdown();

            // once the remaining connections have been shut down, we'll request a graceful shutdown from the network
            // thread pool
            var workerTerminationFuture = workerEventLoopGroup.shutdownGracefully(
                    config.get(GraphDatabaseInternalSettings.netty_server_shutdown_quiet_period),
                    config.get(GraphDatabaseInternalSettings.netty_server_shutdown_timeout)
                            .toSeconds(),
                    TimeUnit.SECONDS);

            var workerTerminationCompleted = workerTerminationFuture.awaitUninterruptibly(
                    config.get(BoltConnectorInternalSettings.thread_pool_shutdown_wait_time)
                            .toSeconds(),
                    TimeUnit.SECONDS);
            if (!workerTerminationCompleted) {
                log.warn(
                        "Termination of worker event loop group has exceeded maximum permitted duration - Remaining jobs will be forcefully terminated");
            } else if (!workerTerminationFuture.isSuccess()) {
                log.warn("Termination of worker event loop group has failed", workerTerminationFuture.cause());
            }

            // also make sure that our executor service is cleanly shut down - there should be no remaining jobs present
            // as connectors will kill any remaining jobs forcefully as part of their shutdown procedures
            var remainingJobs = executorService.shutdownNow();
            if (!remainingJobs.isEmpty()) {
                log.warn("Forcefully killed %d remaining Bolt jobs to fulfill shutdown request", remainingJobs.size());
            }

            log.info("Bolt server has been shut down");
        }

        if (memoryPool != null) {
            memoryPool.close();
        }
    }

    private ByteBufAllocator getBufferAllocator() {
        PooledByteBufAllocator allocator = NETTY_BUF_ALLOCATOR.get();
        var pool = new BoltMemoryPool(memoryPools, allocator.metric());
        connectorLife.add(new BoltMemoryPoolLifeCycleAdapter(pool));
        memoryPool = pool;
        return allocator;
    }

    private void registerConnector(Connector connector) {
        // append a listener which handles the creation of metrics
        connector.registerListener(new MetricsConnectorListener(connectionMetricsMonitor));

        if (config.get(BoltConnectorInternalSettings.enable_response_metrics)) {
            connector.registerListener(new ResponseMetricsConnectorListener(connectionMetricsMonitor));
        }

        // if an authentication timeout has been configured, we'll register a listener which appends the necessary
        // timeout handlers with the network pipelines upon connection creation
        var authenticationTimeout =
                config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout);
        if (!authenticationTimeout.isZero()) {
            connector.registerListener(new AuthenticationTimeoutConnectorListener(
                    authenticationTimeout, logService.getInternalLogProvider()));
        }

        // if keep-alive have been configured, we'll register a listener which appends the necessary handlers to the
        // network pipelines upon connection negotiation
        var keepAliveMechanism = config.get(BoltConnector.connection_keep_alive_type);
        var keepAliveInterval = config.get(BoltConnector.connection_keep_alive).toMillis();
        if (keepAliveMechanism != BoltConnector.KeepAliveRequestType.OFF) {
            connector.registerListener(new KeepAliveConnectorListener(
                    keepAliveMechanism != BoltConnector.KeepAliveRequestType.ALL,
                    keepAliveInterval,
                    logService.getInternalLogProvider()));
        }

        // if read-limit has been configured, we'll register a listener which appends the necessary handlers to the
        // network pipelines upon connection negotiation
        var readLimit = config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes);
        if (readLimit != 0) {
            connector.registerListener(new ReadLimitConnectorListener(readLimit, logService.getInternalLogProvider()));
        }

        // Register the reset message connection listener
        connector.registerListener(new ResetMessageConnectorListener(logService.getInternalLogProvider()));

        connectorLife.add(connector);
    }

    private Connection.Factory createConnectionFactory() {
        return new AtomicSchedulingConnection.Factory(executorService, clock, logService);
    }

    private static Authentication createAuthentication(AuthManager authManager) {
        return new BasicAuthentication(authManager);
    }

    private void enableOcspStapling() {
        if (SslProvider.JDK.equals(config.get(SslSystemSettings.netty_ssl_provider))) {
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
            ConnectorType connectorType,
            ByteBufAllocator allocator) {
        var config = new SocketNettyConnector.SocketConfiguration(
                this.config.get(BoltConnectorInternalSettings.protocol_capture),
                this.config.get(BoltConnectorInternalSettings.protocol_capture_path),
                this.config.get(BoltConnectorInternalSettings.protocol_logging),
                this.config.get(BoltConnectorInternalSettings.protocol_logging_mode),
                this.config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_elements),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_depth),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.streaming_buffer_size),
                this.config.get(BoltConnectorInternalSettings.streaming_flush_threshold),
                this.config.get(BoltConnectorInternalSettings.connection_shutdown_wait_time),
                this.config.get(BoltConnectorInternalSettings.transaction_thread_binding),
                this.config.get(BoltConnectorInternalSettings.thread_binding_timeout),
                this.config.get(BoltConnectorInternalSettings.netty_message_merge_cumulator),
                encryptionRequired,
                sslContext,
                this.config.get(BoltConnectorInternalSettings.tcp_keep_alive));

        return new SocketNettyConnector(
                BoltConnector.NAME,
                bindAddress,
                connectorType,
                connectorPortRegister,
                memoryPool,
                clock,
                allocator,
                bossEventLoopGroup,
                workerEventLoopGroup,
                transport,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                createErrorAccountant(),
                createTrafficAccountant(),
                driverMetricsMonitor,
                config,
                logService.getUserLogProvider(),
                logService.getInternalLogProvider());
    }

    private Connector createDomainSocketConnector(
            Connection.Factory connectionFactory,
            ConnectorTransport transport,
            Authentication authentication,
            ByteBufAllocator allocator) {
        var config = new DomainSocketNettyConnector.DomainSocketConfiguration(
                this.config.get(BoltConnectorInternalSettings.protocol_capture),
                this.config.get(BoltConnectorInternalSettings.protocol_capture_path),
                this.config.get(BoltConnectorInternalSettings.protocol_logging),
                this.config.get(BoltConnectorInternalSettings.protocol_logging_mode),
                this.config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_elements),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_depth),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.streaming_buffer_size),
                this.config.get(BoltConnectorInternalSettings.streaming_flush_threshold),
                this.config.get(BoltConnectorInternalSettings.connection_shutdown_wait_time),
                this.config.get(BoltConnectorInternalSettings.transaction_thread_binding),
                this.config.get(BoltConnectorInternalSettings.thread_binding_timeout),
                this.config.get(BoltConnectorInternalSettings.netty_message_merge_cumulator),
                this.config.get(BoltConnectorInternalSettings.unsupported_loopback_delete));

        var socketFile = this.config.get(BoltConnectorInternalSettings.unsupported_loopback_listen_file);
        if (socketFile == null) {
            throw new IllegalArgumentException(
                    "A file has not been specified for use with the loopback domain socket.");
        }

        return new DomainSocketNettyConnector(
                BoltConnectorInternalSettings.LOOPBACK_NAME,
                socketFile,
                memoryPool,
                clock,
                allocator,
                bossEventLoopGroup,
                bossEventLoopGroup,
                transport,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                createErrorAccountant(),
                driverMetricsMonitor,
                config,
                logService.getUserLogProvider(),
                logService.getInternalLogProvider());
    }

    private Connector createLocalConnector(
            Connection.Factory connectionFactory,
            ConnectorTransport transport,
            Authentication authentication,
            ByteBufAllocator allocator,
            int streamingBufferSize,
            int streamingFlushThreshold) {
        var config = new LocalConfiguration(
                this.config.get(BoltConnectorInternalSettings.protocol_capture),
                this.config.get(BoltConnectorInternalSettings.protocol_capture_path),
                this.config.get(BoltConnectorInternalSettings.protocol_logging),
                this.config.get(BoltConnectorInternalSettings.protocol_logging_mode),
                this.config.get(BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_elements),
                this.config.get(BoltConnectorInternalSettings.bolt_unauth_connection_max_structure_depth),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_outbound_buffer_throttle_max_duration),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_low_water_mark),
                this.config.get(BoltConnectorInternalSettings.bolt_inbound_message_throttle_high_water_mark),
                this.config.get(BoltConnectorInternalSettings.streaming_buffer_size),
                this.config.get(BoltConnectorInternalSettings.streaming_flush_threshold),
                this.config.get(BoltConnectorInternalSettings.connection_shutdown_wait_time),
                this.config.get(BoltConnectorInternalSettings.transaction_thread_binding),
                this.config.get(BoltConnectorInternalSettings.thread_binding_timeout),
                this.config.get(BoltConnectorInternalSettings.netty_message_merge_cumulator));

        var bindAddress = new LocalAddress(this.config.get(BoltConnectorInternalSettings.local_channel_address));

        return new LocalNettyConnector(
                BoltConnectorInternalSettings.LOCAL_NAME,
                bindAddress,
                memoryPool,
                clock,
                allocator,
                bossEventLoopGroup,
                workerEventLoopGroup,
                connectionFactory,
                connectionTracker,
                protocolRegistry,
                authentication,
                authConfigProvider,
                defaultDatabaseResolver,
                connectionHintRegistry,
                transactionManager,
                routingService,
                createErrorAccountant(),
                driverMetricsMonitor,
                logService.getUserLogProvider(),
                logService.getInternalLogProvider(),
                transport,
                config);
    }

    private ErrorAccountant createErrorAccountant() {
        if (!config.get(BoltConnector.enable_error_accounting)) {
            return new NoopErrorAccountant(logService);
        }

        return new CircuitBreakerErrorAccountant(
                config.get(BoltConnector.network_abort_warn_threshold),
                config.get(BoltConnector.network_abort_warn_window_duration).toMillis(),
                config.get(BoltConnector.network_abort_clear_window_duration).toMillis(),
                config.get(BoltConnector.thread_starvation_warn_threshold),
                config.get(BoltConnector.thread_starvation_warn_window_duration).toMillis(),
                config.get(BoltConnector.thread_starvation_clear_window_duration)
                        .toMillis(),
                Clock.systemUTC(),
                logService);
    }

    private TrafficAccountant createTrafficAccountant() {
        var checkPeriod = config.get(BoltConnector.traffic_accounting_check_period);
        if (Duration.ZERO.equals(checkPeriod)) {
            return NoopTrafficAccountant.getInstance();
        }

        return new AtomicTrafficAccountant(
                config.get(BoltConnector.traffic_accounting_check_period).toMillis(),
                config.get(BoltConnector.traffic_accounting_incoming_threshold_mbps),
                config.get(BoltConnector.traffic_accounting_outgoing_threshold_mbps),
                config.get(BoltConnector.traffic_accounting_clear_duration).toMillis(),
                logService);
    }

    private static class BoltMemoryPoolLifeCycleAdapter extends LifecycleAdapter {

        private final NettyMemoryPool pool;

        private BoltMemoryPoolLifeCycleAdapter(NettyMemoryPool pool) {
            this.pool = pool;
        }

        @Override
        public void shutdown() {
            pool.close();
        }
    }
}
