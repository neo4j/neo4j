/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cluster.client;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ConnectedStateMachines;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateTransitionLogger;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

public class ClusterClient extends LifecycleAdapter implements ClusterMonitor, Cluster, AtomicBroadcast
{
    private final LifeSupport life = new LifeSupport();
    private final Cluster cluster;
    private final AtomicBroadcast broadcast;
    private final Heartbeat heartbeat;
    private final ProtocolServer server;

    public interface Configuration
    {
        long getHeartbeatTimeout();

        long getHeartbeatInterval();

        ElectionCredentialsProvider getElectionCredentialsProvider();

        int[] getPorts();

        String getAddress();

        boolean isDiscoveryEnabled();

        String[] getInitialHosts();

        String getDiscoveryUrl();

        String getClusterName();
    }

    public static Configuration adapt( final Config config,
            final ElectionCredentialsProvider electionCredentialsProvider )
    {
        return new Configuration()
        {
            @Override
            public boolean isDiscoveryEnabled()
            {
                return config.get( ClusterSettings.cluster_discovery_enabled );
            }

            @Override
            public int[] getPorts()
            {
                return ClusterSettings.cluster_server.getPorts( config.getParams() );
            }

            @Override
            public String[] getInitialHosts()
            {
                String hosts = config.get( ClusterSettings.initial_hosts );
                return hosts != null ? hosts.split( "," ) : new String[0];
            }

            @Override
            public long getHeartbeatTimeout()
            {
                return 5000;
            }

            @Override
            public long getHeartbeatInterval()
            {
                return 10000;
            }

            @Override
            public ElectionCredentialsProvider getElectionCredentialsProvider()
            {
                return electionCredentialsProvider;
            }

            @Override
            public String getDiscoveryUrl()
            {
                return config.get( ClusterSettings.cluster_discovery_url );
            }

            @Override
            public String getClusterName()
            {
                return config.get( ClusterSettings.cluster_name );
            }

            @Override
            public String getAddress()
            {
                return ClusterSettings.cluster_server.getAddress( config.getParams() );
            }
        };
    }

    public ClusterClient( final Configuration config, final Logging logging )
    {
        MessageTimeoutStrategy timeoutStrategy = new MessageTimeoutStrategy( new FixedTimeoutStrategy(
                config.getHeartbeatTimeout() ) )
                .timeout( HeartbeatMessage.sendHeartbeat, config.getHeartbeatInterval() ).relativeTimeout(
                        HeartbeatMessage.timed_out, HeartbeatMessage.sendHeartbeat, config.getHeartbeatInterval() );

        MultiPaxosServerFactory protocolServerFactory = new MultiPaxosServerFactory( new ClusterConfiguration(
                "neo4j.ha" ), logging );

        InMemoryAcceptorInstanceStore acceptorInstanceStore = new InMemoryAcceptorInstanceStore();
        ElectionCredentialsProvider electionCredentialsProvider = config.getElectionCredentialsProvider();

        NetworkInstance networkNodeTCP = new NetworkInstance( new NetworkInstance.Configuration()
        {
            @Override
            public int[] getPorts()
            {
                return config.getPorts();
            }

            @Override
            public String getAddress()
            {
                return config.getAddress();
            }
        }, StringLogger.SYSTEM );

        server = protocolServerFactory.newProtocolServer( timeoutStrategy, networkNodeTCP, networkNodeTCP,
                acceptorInstanceStore, electionCredentialsProvider );

        networkNodeTCP.addNetworkChannelsListener( new NetworkInstance.NetworkChannelsListener()
        {
            @Override
            public void listeningAt( URI me )
            {
                server.listeningAt( me );
                server.addStateTransitionListener( new StateTransitionLogger( logging ) );
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

        life.add( networkNodeTCP );

        // Timeout timer - triggers every 10 ms
        life.add( new Lifecycle()
        {
            private ScheduledExecutorService scheduler;

            @Override
            public void init() throws Throwable
            {
                server.getTimeouts().tick( System.currentTimeMillis() );
            }

            @Override
            public void start() throws Throwable
            {
                scheduler = Executors.newSingleThreadScheduledExecutor( new DaemonThreadFactory( "timeout" ) );

                scheduler.scheduleWithFixedDelay( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        long now = System.currentTimeMillis();

                        server.getTimeouts().tick( now );
                    }
                }, 0, 10, TimeUnit.MILLISECONDS );
            }

            @Override
            public void stop() throws Throwable
            {
                scheduler.shutdownNow();
            }

            @Override
            public void shutdown() throws Throwable
            {
            }
        } );

        life.add( new ClusterJoin( new ClusterJoin.Configuration()
        {
            @Override
            public boolean isDiscoveryEnabled()
            {
                return config.isDiscoveryEnabled();
            }

            @Override
            public String[] getInitialHosts()
            {
                return config.getInitialHosts();
            }

            @Override
            public String getDiscoveryUrl()
            {
                return config.getDiscoveryUrl();
            }

            @Override
            public String getClusterName()
            {
                return config.getClusterName();
            }
        }, server, logging ) );
        
        cluster = server.newClient( Cluster.class );
        broadcast = server.newClient( AtomicBroadcast.class );
        heartbeat = server.newClient( Heartbeat.class );
    }

    @Override
    public void init() throws Throwable
    {
        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void broadcast( Payload payload )
    {
        broadcast.broadcast( payload );
    }

    @Override
    public void addAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        broadcast.addAtomicBroadcastListener( listener );
    }

    @Override
    public void removeAtomicBroadcastListener( AtomicBroadcastListener listener )
    {
        broadcast.removeAtomicBroadcastListener( listener );
    }

    @Override
    public void create( String clusterName )
    {
        cluster.create( clusterName );
    }

    @Override
    public Future<ClusterConfiguration> join( URI otherServerUrl )
    {
        return cluster.join( otherServerUrl );
    }

    @Override
    public void leave()
    {
        cluster.leave();
    }

    @Override
    public void addClusterListener( ClusterListener listener )
    {
        cluster.addClusterListener( listener );
    }

    @Override
    public void removeClusterListener( ClusterListener listener )
    {
        cluster.removeClusterListener( listener );
    }

    @Override
    public void addHeartbeatListener( HeartbeatListener listener )
    {
        heartbeat.addHeartbeatListener( listener );
    }

    @Override
    public void removeHeartbeatListener( HeartbeatListener listener )
    {
        heartbeat.removeHeartbeatListener( listener );
    }

    @Override
    public void addBindingListener( BindingListener bindingListener )
    {
        server.addBindingListener( bindingListener );
    }
    
    @Override
    public void removeBindingListener( BindingListener listener )
    {
        server.removeBindingListener( listener );
    }

    public void dumpDiagnostics( StringBuilder appendTo )
    {
        ConnectedStateMachines stateMachines = server.getConnectedStateMachines();
        for ( StateMachine stateMachine : stateMachines.getStateMachines() )
        {
            appendTo.append( "   " ).append( stateMachine.getMessageType().getSimpleName() ).append( ":" )
                    .append( stateMachine.getState().toString() ).append( "\n" );
        }

        appendTo.append( "Current timeouts:\n" );
        for ( Map.Entry<Object, Timeouts.Timeout> objectTimeoutEntry : stateMachines.getTimeouts().getTimeouts()
                .entrySet() )
        {
            appendTo.append( objectTimeoutEntry.getKey().toString() ).append( ":" )
                    .append( objectTimeoutEntry.getValue().getTimeoutMessage().toString() );
        }
    }
    
    public URI getServerUri()
    {
        return server.getServerId();
    }
}
