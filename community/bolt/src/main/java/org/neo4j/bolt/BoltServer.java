/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import java.util.Map;

import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltConnectionReadLimiter;
import org.neo4j.bolt.runtime.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.CachedThreadPoolExecutorFactory;
import org.neo4j.bolt.runtime.DefaultBoltConnectionFactory;
import org.neo4j.bolt.runtime.ExecutorBoltSchedulerProvider;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.bolt.transport.BoltProtocolFactory;
import org.neo4j.bolt.transport.DefaultBoltProtocolFactory;
import org.neo4j.bolt.transport.Netty4LoggerFactory;
import org.neo4j.bolt.transport.NettyServer;
import org.neo4j.bolt.transport.NettyServer.ProtocolInitializer;
import org.neo4j.bolt.transport.SocketTransport;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.runtime.BoltStateMachineFactory;
import org.neo4j.bolt.v1.runtime.BoltStateMachineFactoryImpl;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.bolt.BoltConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.udc.UsageData;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.neo4j.scheduler.JobScheduler.Groups.boltNetworkIO;

public class BoltServer extends LifecycleAdapter
{
    // platform dependencies
    private final GraphDatabaseAPI db;
    private final FileSystemAbstraction fs;
    private final JobScheduler jobScheduler;
    private final AvailabilityGuard availabilityGuard;
    private final ConnectorPortRegister connectorPortRegister;
    private final UsageData usageData;
    private final Config config;
    private final Clock clock;
    private final Monitors monitors;
    private final LogService logService;

    // edition specific dependencies are resolved dynamically
    private final DependencyResolver dependencyResolver;

    private final LifeSupport life = new LifeSupport();

    public BoltServer( GraphDatabaseAPI db, FileSystemAbstraction fs, JobScheduler jobScheduler, AvailabilityGuard availabilityGuard,
            ConnectorPortRegister connectorPortRegister, UsageData usageData, Config config, Clock clock, Monitors monitors,
            LogService logService, DependencyResolver dependencyResolver )
    {
        this.db = db;
        this.fs = fs;
        this.jobScheduler = jobScheduler;
        this.availabilityGuard = availabilityGuard;
        this.connectorPortRegister = connectorPortRegister;
        this.usageData = usageData;
        this.config = config;
        this.clock = clock;
        this.monitors = monitors;
        this.logService = logService;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void start() throws Throwable
    {
        Log log = logService.getInternalLog( BoltServer.class );
        Log userLog = logService.getUserLog( BoltServer.class );

        InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logService.getInternalLogProvider() ) );
        BoltMessageLogging boltLogging = BoltMessageLogging.create( fs, jobScheduler, config, log );
        life.add( boltLogging );

        Authentication authentication = createAuthentication();

        TransportThrottleGroup throttleGroup = new TransportThrottleGroup( config, clock );

        BoltSchedulerProvider boltSchedulerProvider =
                life.add( new ExecutorBoltSchedulerProvider( config, new CachedThreadPoolExecutorFactory( log ), jobScheduler, logService ) );
        BoltConnectionFactory boltConnectionFactory =
                createConnectionFactory( config, boltSchedulerProvider, throttleGroup, logService, clock );
        BoltStateMachineFactory boltStateMachineFactory = createBoltFactory( authentication, clock );

        BoltProtocolFactory boltProtocolInstaller = createBoltProtocolInstallerFactory( boltConnectionFactory, boltStateMachineFactory );

        if ( !config.enabledBoltConnectors().isEmpty() && !config.get( GraphDatabaseSettings.disconnected ) )
        {
            NettyServer server = new NettyServer( jobScheduler.threadFactory( boltNetworkIO ),
                    createConnectors( boltProtocolInstaller, throttleGroup, boltLogging, log ), connectorPortRegister, userLog );
            life.add( server );
            log.info( "Bolt server loaded" );
        }

        life.start(); // init and start the nested lifecycle
    }

    @Override
    public void stop() throws Throwable
    {
        life.shutdown(); // stop and shutdown the nested lifecycle
    }

    private BoltConnectionFactory createConnectionFactory( Config config, BoltSchedulerProvider schedulerProvider,
            TransportThrottleGroup throttleGroup, LogService logService, Clock clock )
    {
        return new DefaultBoltConnectionFactory( schedulerProvider, throttleGroup, logService, clock,
                new BoltConnectionReadLimiter( logService.getInternalLog( BoltConnectionReadLimiter.class ),
                        config.get( GraphDatabaseSettings.bolt_inbound_message_throttle_low_water_mark ),
                        config.get( GraphDatabaseSettings.bolt_inbound_message_throttle_high_water_mark ) ), monitors );
    }

    private Map<BoltConnector,ProtocolInitializer> createConnectors( BoltProtocolFactory handlerFactory,
            TransportThrottleGroup throttleGroup, BoltMessageLogging boltLogging, Log log )
    {
        return config.enabledBoltConnectors()
                .stream()
                .collect( toMap( identity(), connector -> createProtocolInitializer( connector, handlerFactory, throttleGroup, boltLogging, log ) ) );
    }

    private ProtocolInitializer createProtocolInitializer( BoltConnector connector, BoltProtocolFactory handlerFactory,
            TransportThrottleGroup throttleGroup, BoltMessageLogging boltLogging, Log log )
    {
        SslContext sslCtx;
        boolean requireEncryption;
        BoltConnector.EncryptionLevel encryptionLevel = config.get( connector.encryption_level );
        SslPolicyLoader sslPolicyLoader = dependencyResolver.resolveDependency( SslPolicyLoader.class );

        switch ( encryptionLevel )
        {
        case REQUIRED:
            // Encrypted connections are mandatory, a self-signed certificate may be generated.
            requireEncryption = true;
            sslCtx = createSslContext( sslPolicyLoader, config );
            break;
        case OPTIONAL:
            // Encrypted connections are optional, a self-signed certificate may be generated.
            requireEncryption = false;
            sslCtx = createSslContext( sslPolicyLoader, config );
            break;
        case DISABLED:
            // Encryption is turned off, no self-signed certificate will be generated.
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

        ListenSocketAddress listenAddress = config.get( connector.listen_address );
        return new SocketTransport( connector.key(), listenAddress, sslCtx, requireEncryption, logService.getInternalLogProvider(), boltLogging,
                throttleGroup, handlerFactory );
    }

    private static SslContext createSslContext( SslPolicyLoader sslPolicyFactory, Config config )
    {
        try
        {
            String policyName = config.get( GraphDatabaseSettings.bolt_ssl_policy );
            if ( policyName == null )
            {
                throw new IllegalArgumentException( "No SSL policy has been configured for Bolt server" );
            }
            return sslPolicyFactory.getPolicy( policyName ).nettyServerContext();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to initialize SSL encryption support, which is required to start this connector. " +
                                        "Error was: " + e.getMessage(), e );
        }
    }

    private Authentication createAuthentication()
    {
        return new BasicAuthentication( dependencyResolver.resolveDependency( AuthManager.class ),
                dependencyResolver.resolveDependency( UserManagerSupplier.class ) );
    }

    private BoltProtocolFactory createBoltProtocolInstallerFactory( BoltConnectionFactory connectionFactory,
            BoltStateMachineFactory stateMachineFactory )
    {
        return new DefaultBoltProtocolFactory( connectionFactory, stateMachineFactory, logService );
    }

    private BoltStateMachineFactory createBoltFactory( Authentication authentication, Clock clock )
    {
        BoltConnectionTracker connectionTracker = dependencyResolver.resolveDependency( BoltConnectionTracker.class );
        return new BoltStateMachineFactoryImpl( db, usageData, availabilityGuard, authentication, connectionTracker, clock, config, logService );
    }
}
