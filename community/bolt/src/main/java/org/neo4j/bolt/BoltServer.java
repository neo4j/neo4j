/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.time.Clock;

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
import org.neo4j.bolt.transport.BoltProtocolFactory;
import org.neo4j.bolt.transport.DefaultBoltProtocolFactory;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.ssl.SslPolicyScope.BOLT;

public class BoltServer extends LifecycleAdapter
{
    private final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    private final JobScheduler jobScheduler;
    private final ConnectorPortRegister connectorPortRegister;
    private final NetworkConnectionTracker connectionTracker;
    private final DatabaseIdRepository databaseIdRepository;
    private final Config config;
    private final SystemNanoClock clock;
    private final Monitors monitors;
    private final LogService logService;
    private final AuthManager authManager;

    // edition specific dependencies are resolved dynamically
    private final DependencyResolver dependencyResolver;

    private final LifeSupport life = new LifeSupport();

    public BoltServer( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI, JobScheduler jobScheduler,
            ConnectorPortRegister connectorPortRegister, NetworkConnectionTracker connectionTracker,
            DatabaseIdRepository databaseIdRepository, Config config, SystemNanoClock clock,
            Monitors monitors, LogService logService, DependencyResolver dependencyResolver, AuthManager authManager )
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
        this.authManager = authManager;
    }

    @Override
    public void init()
    {
        Log log = logService.getInternalLog( BoltServer.class );

        InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logService.getInternalLogProvider() ) );

        Authentication authentication = createAuthentication();

        TransportThrottleGroup throttleGroup = new TransportThrottleGroup( config, clock );

        BoltSchedulerProvider boltSchedulerProvider =
                life.setLast( new ExecutorBoltSchedulerProvider( config, new CachedThreadPoolExecutorFactory(), jobScheduler, logService ) );
        BoltConnectionFactory boltConnectionFactory =
                createConnectionFactory( config, boltSchedulerProvider, throttleGroup, logService, clock );
        BoltStateMachineFactory boltStateMachineFactory = createBoltStateMachineFactory( authentication, clock );

        BoltProtocolFactory boltProtocolFactory = createBoltProtocolFactory( boltConnectionFactory, boltStateMachineFactory );

        if ( config.get( BoltConnector.enabled ) )
        {
            jobScheduler.setThreadFactory( Group.BOLT_NETWORK_IO, NettyThreadFactory::new );
            NettyServer server = new NettyServer( jobScheduler.threadFactory( Group.BOLT_NETWORK_IO ),
                    createProtocolInitializer( boltProtocolFactory, throttleGroup, log ), connectorPortRegister, logService );
            life.add( server );
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
            TransportThrottleGroup throttleGroup, LogService logService, Clock clock )
    {
        return new DefaultBoltConnectionFactory( schedulerProvider, throttleGroup, config, logService, clock, monitors );
    }

    private ProtocolInitializer createProtocolInitializer( BoltProtocolFactory boltProtocolFactory,
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
        return new SocketTransport( BoltConnector.NAME, listenAddress, sslCtx, requireEncryption, logService.getInternalLogProvider(),
                throttleGroup, boltProtocolFactory, connectionTracker );
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

    private Authentication createAuthentication()
    {
        return new BasicAuthentication( authManager );
    }

    private BoltProtocolFactory createBoltProtocolFactory( BoltConnectionFactory connectionFactory, BoltStateMachineFactory stateMachineFactory )
    {
        var customBookmarkParser = boltGraphDatabaseManagementServiceSPI.getCustomBookmarkFormatParser().orElse( CustomBookmarkFormatParser.DEFAULT );
        return new DefaultBoltProtocolFactory( connectionFactory, stateMachineFactory, logService, databaseIdRepository, customBookmarkParser );
    }

    private BoltStateMachineFactory createBoltStateMachineFactory( Authentication authentication, SystemNanoClock clock )
    {
        return new BoltStateMachineFactoryImpl( boltGraphDatabaseManagementServiceSPI, authentication, clock, config, logService );
    }
}
