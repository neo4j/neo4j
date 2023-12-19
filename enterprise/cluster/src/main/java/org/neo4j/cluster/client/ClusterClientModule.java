/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.client;

import org.jboss.netty.logging.InternalLoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ExecutorLifecycleAdapter;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.logging.AsyncLogging;
import org.neo4j.cluster.logging.NettyLoggerFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionMessage;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.statemachine.StateTransitionLogger;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.NamedThreadFactory.daemon;

/**
 * This is a builder for {@link ClusterClient} instances.
 * <p/>
 * While a {@link Dependencies} instance is passed into the constructor there are no services other than those
 * explicitly passed in that are required, and instead it is only used to register any created services for others to
 * use.
 */
public class ClusterClientModule
{
    public final ClusterClient clusterClient;
    private final ProtocolServer server;

    public ClusterClientModule( LifeSupport life, Dependencies dependencies, final Monitors monitors,
            final Config config, LogService logService, ElectionCredentialsProvider electionCredentialsProvider )
    {
        final LogProvider logging = AsyncLogging.provider( life, logService.getInternalLogProvider() );
        InternalLoggerFactory.setDefaultFactory( new NettyLoggerFactory( logging ) );

        TimeoutStrategy timeoutStrategy = new MessageTimeoutStrategy(
                new FixedTimeoutStrategy( config.get( ClusterSettings.default_timeout ).toMillis() ) )
                .timeout( HeartbeatMessage.sendHeartbeat, config.get( ClusterSettings.heartbeat_interval ).toMillis() )
                .timeout( HeartbeatMessage.timed_out, config.get( ClusterSettings.heartbeat_timeout ).toMillis() )
                .timeout( AtomicBroadcastMessage.broadcastTimeout, config.get( ClusterSettings.broadcast_timeout ).toMillis() )
                .timeout( LearnerMessage.learnTimedout, config.get( ClusterSettings.learn_timeout ).toMillis() )
                .timeout( ProposerMessage.phase1Timeout, config.get( ClusterSettings.phase1_timeout ).toMillis() )
                .timeout( ProposerMessage.phase2Timeout, config.get( ClusterSettings.phase2_timeout ).toMillis() )
                .timeout( ClusterMessage.joiningTimeout, config.get( ClusterSettings.join_timeout ).toMillis() )
                .timeout( ClusterMessage.configurationTimeout, config.get( ClusterSettings.configuration_timeout ).toMillis() )
                .timeout( ClusterMessage.leaveTimedout, config.get( ClusterSettings.leave_timeout ).toMillis() )
                .timeout( ElectionMessage.electionTimeout, config.get( ClusterSettings.election_timeout ).toMillis() );

        MultiPaxosServerFactory protocolServerFactory = new MultiPaxosServerFactory(
                new ClusterConfiguration( config.get( ClusterSettings.cluster_name ), logging ),
                logging, monitors.newMonitor( StateMachines.Monitor.class ) );

        NetworkReceiver receiver = dependencies.satisfyDependency(
                new NetworkReceiver( monitors.newMonitor( NetworkReceiver.Monitor.class ),
                new NetworkReceiver.Configuration()
        {
            @Override
            public HostnamePort clusterServer()
            {
                return config.get( ClusterSettings.cluster_server );
            }

            @Override
            public int defaultPort()
            {
                return 5001;
            }

            @Override
            public String name()
            {
                return config.get( ClusterSettings.instance_name );
            }
        }, logging ));

        final ObjectInputStreamFactory objectInputStreamFactory = new ObjectStreamFactory();
        final ObjectOutputStreamFactory objectOutputStreamFactory = new ObjectStreamFactory();

        receiver.addNetworkChannelsListener( new NetworkReceiver.NetworkChannelsListener()
        {
            private volatile StateTransitionLogger logger;

            @Override
            public void listeningAt( URI me )
            {
                server.listeningAt( me );
                if ( logger == null )
                {
                    logger = new StateTransitionLogger( logging,
                            new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory ) );
                    server.addStateTransitionListener( logger );
                }
            }

            @Override
            public void channelOpened( URI to )
            {
                logging.getLog( NetworkReceiver.class ).info( to + " connected to me at " + server.boundAt() );
            }

            @Override
            public void channelClosed( URI to )
            {
                logging.getLog( NetworkReceiver.class ).info( to + " disconnected from me at " + server
                        .boundAt() );
            }
        } );

        NetworkSender sender = dependencies.satisfyDependency(new NetworkSender( monitors.newMonitor( NetworkSender.Monitor.class ),
                new NetworkSender.Configuration()
        {
            @Override
            public int defaultPort()
            {
                return 5001;
            }

            @Override
            public int port()
            {
                return config.get( ClusterSettings.cluster_server ).getPort();
            }
        }, receiver, logging ));

        ExecutorLifecycleAdapter stateMachineExecutor = new ExecutorLifecycleAdapter(
                () -> Executors.newSingleThreadExecutor( new NamedThreadFactory( "State machine", monitors
                        .newMonitor( NamedThreadFactory.Monitor.class ) ) ) );

        AcceptorInstanceStore acceptorInstanceStore = new InMemoryAcceptorInstanceStore();

        server = protocolServerFactory.newProtocolServer( config.get( ClusterSettings.server_id ),timeoutStrategy,
                receiver, sender, acceptorInstanceStore, electionCredentialsProvider, stateMachineExecutor,
                objectInputStreamFactory, objectOutputStreamFactory, config );

        life.add( sender );
        life.add( stateMachineExecutor );
        life.add( receiver );

        // Timeout timer - triggers every 10 ms
        life.add( new TimeoutTrigger( server, monitors ) );

        life.add( new ClusterJoin( new ClusterJoin.Configuration()
        {
            @Override
            public List<HostnamePort> getInitialHosts()
            {
                return config.get( ClusterSettings.initial_hosts );
            }

            @Override
            public String getClusterName()
            {
                return config.get( ClusterSettings.cluster_name );
            }

            @Override
            public boolean isAllowedToCreateCluster()
            {
                return config.get( ClusterSettings.allow_init_cluster );
            }

            @Override
            public long getClusterJoinTimeout()
            {
                return config.get( ClusterSettings.join_timeout ).toMillis();
            }
        }, server, logService ) );

        clusterClient =  dependencies.satisfyDependency(new ClusterClient( life, server ));
    }

    private static class TimeoutTrigger implements Lifecycle
    {
        private final ProtocolServer server;
        private final Monitors monitors;

        private ScheduledExecutorService scheduler;
        private ScheduledFuture<?> tickFuture;

        TimeoutTrigger( ProtocolServer server, Monitors monitors )
        {
            this.server = server;
            this.monitors = monitors;
        }

        @Override
        public void init()
        {
            server.getTimeouts().tick( System.currentTimeMillis() );
        }

        @Override
        public void start()
        {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                    daemon( "timeout-clusterClient", monitors.newMonitor( NamedThreadFactory.Monitor.class ) ) );

            tickFuture = scheduler.scheduleWithFixedDelay( () ->
            {
                long now = System.currentTimeMillis();

                server.getTimeouts().tick( now );
            }, 0, 10, TimeUnit.MILLISECONDS );
        }

        @Override
        public void stop()
        {
            tickFuture.cancel( true );
            scheduler.shutdownNow();
        }

        @Override
        public void shutdown()
        {
        }
    }
}
