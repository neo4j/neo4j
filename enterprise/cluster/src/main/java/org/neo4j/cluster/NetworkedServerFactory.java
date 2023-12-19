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
package org.neo4j.cluster;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AcceptorInstanceStore;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.statemachine.StateTransitionLogger;
import org.neo4j.cluster.timeout.TimeoutStrategy;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.LogProvider;

/**
 * TODO
 */
public class NetworkedServerFactory
{
    private LifeSupport life;
    private ProtocolServerFactory protocolServerFactory;
    private TimeoutStrategy timeoutStrategy;
    private final NetworkReceiver.Monitor networkReceiverMonitor;
    private final NetworkSender.Monitor networkSenderMonitor;
    private LogProvider logProvider;
    private ObjectInputStreamFactory objectInputStreamFactory;
    private ObjectOutputStreamFactory objectOutputStreamFactory;
    private final NamedThreadFactory.Monitor namedThreadFactoryMonitor;

    public NetworkedServerFactory( LifeSupport life, ProtocolServerFactory protocolServerFactory,
                                   TimeoutStrategy timeoutStrategy,
                                   LogProvider logProvider,
                                   ObjectInputStreamFactory objectInputStreamFactory,
                                   ObjectOutputStreamFactory objectOutputStreamFactory,
                                   NetworkReceiver.Monitor networkReceiverMonitor,
                                   NetworkSender.Monitor networkSenderMonitor,
                                   NamedThreadFactory.Monitor namedThreadFactoryMonitor )
    {
        this.life = life;
        this.protocolServerFactory = protocolServerFactory;
        this.timeoutStrategy = timeoutStrategy;
        this.networkReceiverMonitor = networkReceiverMonitor;
        this.networkSenderMonitor = networkSenderMonitor;
        this.logProvider = logProvider;
        this.objectInputStreamFactory = objectInputStreamFactory;
        this.objectOutputStreamFactory = objectOutputStreamFactory;
        this.namedThreadFactoryMonitor = namedThreadFactoryMonitor;
    }

    public ProtocolServer newNetworkedServer( final Config config, AcceptorInstanceStore acceptorInstanceStore,
                                              ElectionCredentialsProvider electionCredentialsProvider )
    {
        final NetworkReceiver receiver = new NetworkReceiver( networkReceiverMonitor,
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
                return null;
            }
        }, logProvider );

        final NetworkSender sender = new NetworkSender( networkSenderMonitor, new NetworkSender.Configuration()
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
        }, receiver, logProvider );

        ExecutorLifecycleAdapter stateMachineExecutor = new ExecutorLifecycleAdapter( () ->
                Executors.newSingleThreadExecutor(
                        new NamedThreadFactory( "State machine", namedThreadFactoryMonitor ) ) );

        final ProtocolServer protocolServer = protocolServerFactory.newProtocolServer(
                config.get( ClusterSettings.server_id ), timeoutStrategy, receiver, sender,
                acceptorInstanceStore, electionCredentialsProvider, stateMachineExecutor, objectInputStreamFactory,
                objectOutputStreamFactory, config );
        receiver.addNetworkChannelsListener( new NetworkReceiver.NetworkChannelsListener()
        {
            private StateTransitionLogger logger;

            @Override
            public void listeningAt( URI me )
            {
                protocolServer.listeningAt( me );
                if ( logger == null )
                {
                    logger = new StateTransitionLogger( logProvider,
                            new AtomicBroadcastSerializer( objectInputStreamFactory, objectOutputStreamFactory ) );
                    protocolServer.addStateTransitionListener( logger );
                }
            }

            @Override
            public void channelOpened( URI to )
            {
            }

            @Override
            public void channelClosed( URI to )
            {
            }
        } );

        life.add( stateMachineExecutor );

        // Timeout timer - triggers every 10 ms
        life.add( new Lifecycle()
        {
            private ScheduledExecutorService scheduler;

            @Override
            public void init()
            {
                protocolServer.getTimeouts().tick( System.currentTimeMillis() );
            }

            @Override
            public void start()
            {
                scheduler = Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "timeout" ) );

                scheduler.scheduleWithFixedDelay( () ->
                {
                    long now = System.currentTimeMillis();

                    protocolServer.getTimeouts().tick( now );
                }, 0, 10, TimeUnit.MILLISECONDS );
            }

            @Override
            public void stop()
            {
                scheduler.shutdownNow();
            }

            @Override
            public void shutdown()
            {
            }
        } );

        // Add this last to ensure that timeout service is setup first
        life.add( sender );
        life.add( receiver );

        return protocolServer;
    }
}
