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
package org.neo4j.bolt;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.time.Clock;
import java.time.Duration;
import javax.net.ssl.SSLException;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.DefaultBoltConnectionFactory;
import org.neo4j.bolt.runtime.scheduling.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.scheduling.CachedThreadPoolExecutorFactory;
import org.neo4j.bolt.runtime.scheduling.ExecutorBoltSchedulerProvider;
import org.neo4j.bolt.runtime.scheduling.NettyThreadFactory;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineFactory;
import org.neo4j.bolt.runtime.statemachine.impl.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.transport.BoltNettyMemoryPool;
import org.neo4j.bolt.transport.BoltProtocolFactory;
import org.neo4j.bolt.transport.DefaultBoltProtocolFactory;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SslSystemSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

public class BoltServer extends LifecycleAdapter
{
    private static final PooledByteBufAllocator NETTY_BUF_ALLOCATOR = new PooledByteBufAllocator( PlatformDependent.directBufferPreferred() );
    private final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    private final JobScheduler jobScheduler;
    private final ConnectorPortRegister connectorPortRegister;
    private final NetworkConnectionTracker connectionTracker;
    private final DatabaseIdRepository databaseIdRepository;
    private final Config config;
    private final SystemNanoClock clock;
    private final Monitors monitors;
    private final LogService logService;
    private final AuthManager externalAuthManager;
    private final AuthManager internalAuthManager;
    private final MemoryPools memoryPools;
    private final DefaultDatabaseResolver defaultDatabaseResolver;

    // edition specific dependencies are resolved dynamically
    private final DependencyResolver dependencyResolver;

    private final LifeSupport life = new LifeSupport();

    public BoltServer( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, JobScheduler jobScheduler,
                       ConnectorPortRegister connectorPortRegister, NetworkConnectionTracker connectionTracker,
                       DatabaseIdRepository databaseIdRepository, Config config, SystemNanoClock clock,
                       Monitors monitors, LogService logService, DependencyResolver dependencyResolver,
                       AuthManager externalAuthManager, AuthManager internalAuthManager, MemoryPools memoryPools,
                       DefaultDatabaseResolver defaultDatabaseResolver )
    {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.jobScheduler = jobScheduler;
        this.connectorPortRegister = connectorPortRegister;
        this.connectionTracker = connectionTracker;
        this.databaseIdRepository = databaseIdRepository;
        this.config = config;
        this.clock = clock;
        this.monitors = monitors;
        this.logService = logService;
        this.dependencyResolver = dependencyResolver;
        this.externalAuthManager = externalAuthManager;
        this.internalAuthManager = internalAuthManager;
        this.memoryPools = memoryPools;
        this.defaultDatabaseResolver = defaultDatabaseResolver;
    }

    @Override
    public void init()
    {
        Log log = logService.getInternalLog( BoltServer.class );

        InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logService.getInternalLogProvider() ) );

        TransportThrottleGroup throttleGroup = new TransportThrottleGroup( config, clock );

        BoltSchedulerProvider boltSchedulerProvider =
                life.setLast( new ExecutorBoltSchedulerProvider( config, new CachedThreadPoolExecutorFactory(),
                        jobScheduler, logService ) );
        BoltConnectionFactory boltConnectionFactory = createConnectionFactory( config, boltSchedulerProvider, logService, clock );
        BoltStateMachineFactory externalBoltStateMachineFactory = createBoltStateMachineFactory( createAuthentication( externalAuthManager ), clock );
        BoltStateMachineFactory internalBoltStateMachineFactory = createBoltStateMachineFactory( createAuthentication( internalAuthManager ), clock );

        BoltProtocolFactory externalBoltProtocolFactory = createBoltProtocolFactory( boltConnectionFactory, externalBoltStateMachineFactory, throttleGroup,
                                                                                     clock, config.get( BoltConnectorInternalSettings.connection_keep_alive ) );
        BoltProtocolFactory internalBoltProtocolFactory = createBoltProtocolFactory( boltConnectionFactory, internalBoltStateMachineFactory, throttleGroup,
                                                                                     clock, config.get( BoltConnectorInternalSettings.connection_keep_alive ) );

        if ( config.get( BoltConnector.ocsp_enabled ) )
        {
            enableOcspStapling();
        }

        if ( config.get( BoltConnector.enabled ) )
        {
            jobScheduler.setThreadFactory( Group.BOLT_NETWORK_IO, NettyThreadFactory::new );
            NettyServer nettyServer;

            if ( config.get( GraphDatabaseSettings.routing_enabled ) )
            {
                nettyServer = new NettyServer( jobScheduler.threadFactory( Group.BOLT_NETWORK_IO ),
                                               createExternalProtocolInitializer( externalBoltProtocolFactory, throttleGroup, log ),
                                               createInternalProtocolInitializer( internalBoltProtocolFactory, throttleGroup ),
                                               connectorPortRegister,
                                               logService, config );
            }
            else
            {
                nettyServer = new NettyServer( jobScheduler.threadFactory( Group.BOLT_NETWORK_IO ),
                                               createExternalProtocolInitializer( externalBoltProtocolFactory, throttleGroup, log ),
                                               connectorPortRegister,
                                               logService, config );
            }

            var boltMemoryPool = new BoltNettyMemoryPool( memoryPools, NETTY_BUF_ALLOCATOR.metric() );

            life.add( new BoltMemoryPoolLifeCycleAdapter( boltMemoryPool ) );
            life.add( nettyServer );
            log.info( "Bolt server loaded" );
        }

        life.init();
    }

    @Override
    public void start() throws Exception
    {
        life.start(); // init and start the nested lifecycle
    }

    @Override
    public void stop() throws Exception
    {
        life.stop(); // stop the nested lifecycle
    }

    @Override
    public void shutdown()
    {
        life.shutdown(); // shutdown the nested lifecycle
    }

    private BoltConnectionFactory createConnectionFactory( Config config, BoltSchedulerProvider schedulerProvider,
            LogService logService, Clock clock )
    {
        return new DefaultBoltConnectionFactory( schedulerProvider, config, logService, clock, monitors );
    }

    private ProtocolInitializer createInternalProtocolInitializer( BoltProtocolFactory boltProtocolFactory, TransportThrottleGroup throttleGroup )

    {
        SslContext sslCtx = null;
        SslPolicyLoader sslPolicyLoader = dependencyResolver.resolveDependency( SslPolicyLoader.class );

        boolean requireEncryption = sslPolicyLoader.hasPolicyForSource( CLUSTER );

        if ( requireEncryption )
        {
            try
            {
                sslCtx = sslPolicyLoader.getPolicy( CLUSTER ).nettyServerContext();
            }
            catch ( SSLException e )
            {
                throw new RuntimeException( "Failed to initialize SSL encryption support, which is required to start this connector. " +
                                            "Error was: " + e.getMessage(), e );
            }
        }

        SocketAddress internalListenAddress;

        if ( config.isExplicitlySet( GraphDatabaseSettings.routing_listen_address ) )
        {
            internalListenAddress = config.get( GraphDatabaseSettings.routing_listen_address );
        }
        else
        {
            // otherwise use same host as external connector but with default internal port
            internalListenAddress = new SocketAddress( config.get( BoltConnector.listen_address ).getHostname(),
                                                       config.get( GraphDatabaseSettings.routing_listen_address ).getPort() );
        }

        Duration channelTimeout = config.get( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout );
        long maxMessageSize = config.get( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes );

        return new SocketTransport( BoltConnector.NAME, internalListenAddress, sslCtx, requireEncryption, logService.getInternalLogProvider(),
                                    throttleGroup, boltProtocolFactory, connectionTracker, channelTimeout, maxMessageSize, BoltServer.NETTY_BUF_ALLOCATOR );
    }

    private ProtocolInitializer createExternalProtocolInitializer( BoltProtocolFactory boltProtocolFactory,
                                                                   TransportThrottleGroup throttleGroup, Log log )
    {
        SslContext sslCtx;
        boolean requireEncryption;
        BoltConnector.EncryptionLevel encryptionLevel = config.get( BoltConnector.encryption_level );
        SslPolicyLoader sslPolicyLoader = dependencyResolver.resolveDependency( SslPolicyLoader.class );

        switch ( encryptionLevel )
        {
        case REQUIRED:
            // Encrypted connections are mandatory.
            requireEncryption = true;
            sslCtx = createSslContext( sslPolicyLoader );
            break;
        case OPTIONAL:
            // Encrypted connections are optional.
            requireEncryption = false;
            sslCtx = createSslContext( sslPolicyLoader );
            break;
        case DISABLED:
            // Encryption is turned off.
            requireEncryption = false;
            sslCtx = null;
            break;
        default:
            // In the unlikely event that we happen to fall through to the default option here,
            // there is a mismatch between the BoltConnector.EncryptionLevel enum and the options
            // handled in this switch statement. In this case, we'll log a warning and default to
            // disabling encryption, since this mirrors the functionality introduced in 3.0.
            log.warn( "Unhandled encryption level %s - assuming DISABLED.", encryptionLevel.name() );
            requireEncryption = false;
            sslCtx = null;
            break;
        }

        SocketAddress listenAddress = config.get( BoltConnector.listen_address );
        Duration channelTimeout = config.get( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_timeout );
        long maxMessageSize = config.get( BoltConnectorInternalSettings.unsupported_bolt_unauth_connection_max_inbound_bytes );

        return new SocketTransport( BoltConnector.NAME, listenAddress, sslCtx, requireEncryption, logService.getInternalLogProvider(),
                                    throttleGroup, boltProtocolFactory, connectionTracker, channelTimeout, maxMessageSize, BoltServer.NETTY_BUF_ALLOCATOR );
    }

    private static SslContext createSslContext( SslPolicyLoader sslPolicyFactory )
    {
        try
        {
            if ( !sslPolicyFactory.hasPolicyForSource( BOLT ) )
            {
                throw new IllegalArgumentException( "No SSL policy has been configured for Bolt server" );
            }
            return sslPolicyFactory.getPolicy( BOLT ).nettyServerContext();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to initialize SSL encryption support, which is required to start this connector. " +
                    "Error was: " + e.getMessage(), e );
        }
    }

    private void enableOcspStapling()
    {
        if ( SslProvider.JDK.equals( config.get( SslSystemSettings.netty_ssl_provider ) ) )
        {
            // currently the only way to enable OCSP server stapling for JDK is through this property
            System.setProperty( "jdk.tls.server.enableStatusRequestExtension", "true" );
        }
        else
        {
            throw new IllegalArgumentException( "OCSP Server stapling can only be used with JDK ssl provider (see " +
                                        SslSystemSettings.netty_ssl_provider.name() + ")" );
        }
    }

    private Authentication createAuthentication( AuthManager authManager )
    {
        return new BasicAuthentication( authManager );
    }

    private BoltProtocolFactory createBoltProtocolFactory( BoltConnectionFactory connectionFactory,
            BoltStateMachineFactory stateMachineFactory, TransportThrottleGroup throttleGroup, SystemNanoClock clock,
            Duration keepAliveInterval )
    {
        var customBookmarkParser = boltGraphDatabaseManagementServiceSPI.getCustomBookmarkFormatParser()
                .orElse( CustomBookmarkFormatParser.DEFAULT );
        return new DefaultBoltProtocolFactory( connectionFactory, stateMachineFactory, config, logService,
                databaseIdRepository, customBookmarkParser, throttleGroup, clock, keepAliveInterval );
    }

    private BoltStateMachineFactory createBoltStateMachineFactory( Authentication authentication, SystemNanoClock clock )
    {
        return new BoltStateMachineFactoryImpl( boltGraphDatabaseManagementServiceSPI, authentication, clock, config, logService, defaultDatabaseResolver );
    }

    private static class BoltMemoryPoolLifeCycleAdapter extends LifecycleAdapter
    {
        private final BoltNettyMemoryPool pool;

        private BoltMemoryPoolLifeCycleAdapter( BoltNettyMemoryPool pool )
        {
            this.pool = pool;
        }

        @Override
        public void shutdown()
        {
            pool.close();
        }
    }
}
