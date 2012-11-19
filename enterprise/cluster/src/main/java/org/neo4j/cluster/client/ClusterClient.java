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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ConnectedStateMachines;
import org.neo4j.cluster.MultiPaxosServerFactory;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerMessage;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.election.ElectionCredentialsProvider;
import org.neo4j.cluster.protocol.election.ElectionMessage;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatMessage;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.statemachine.StateTransitionLogger;
import org.neo4j.cluster.timeout.FixedTimeoutStrategy;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

public class ClusterClient extends LifecycleAdapter implements Cluster, AtomicBroadcast, Heartbeat, Snapshot
{
    public interface Configuration
    {
        HostnamePort getAddress();

        boolean clusterDiscoveryEnabled();

        List<HostnamePort> getInitialHosts();

        String getDiscoveryUrl();

        String getClusterName();

        boolean isAllowedToCreateCluster();

        // Cluster timeout settings
        long defaultTimeout(); // default is 5s

        long heartbeatInterval(); // inherits defaultTimeout

        long heartbeatTimeout(); // heartbeatInterval * 2 by default

        long broadcastTimeout(); // default is 30s

        long learnTimeout(); // inherits defaultTimeout

        long paxosTimeout(); // inherits defaultTimeout

        long phase1Timeout(); // inherits paxosTimeout

        long phase2Timeout(); // inherits paxosTimeout

        long joinTimeout(); // inherits defaultTimeout

        long configurationTimeout(); // inherits defaultTimeout

        long leaveTimeout(); // inherits paxosTimeout

        long electionTimeout(); // inherits paxosTimeout
    }

    public static Configuration adapt( final Config config )
    {
        return new Configuration()
        {
            @Override
            public boolean clusterDiscoveryEnabled()
            {
                return config.get( ClusterSettings.cluster_discovery_enabled );
            }

            @Override
            public List<HostnamePort> getInitialHosts()
            {
                return config.get( ClusterSettings.initial_hosts );
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
            public HostnamePort getAddress()
            {
                return config.get( ClusterSettings.cluster_server );
            }

            @Override
            public boolean isAllowedToCreateCluster()
            {
                return config.get( ClusterSettings.allow_init_cluster );
            }

            // Timeouts
            @Override
            public long defaultTimeout()
            {
                return config.get( ClusterSettings.default_timeout );
            }

            @Override
            public long heartbeatTimeout()
            {
                return config.get( ClusterSettings.heartbeat_timeout );
            }

            @Override
            public long heartbeatInterval()
            {
                return config.get( ClusterSettings.heartbeat_interval );
            }

            @Override
            public long joinTimeout()
            {
                return config.get( ClusterSettings.join_timeout );
            }

            @Override
            public long configurationTimeout()
            {
                return config.get( ClusterSettings.configuration_timeout );
            }

            @Override
            public long leaveTimeout()
            {
                return config.get( ClusterSettings.leave_timeout );
            }

            @Override
            public long electionTimeout()
            {
                return config.get( ClusterSettings.election_timeout );
            }

            @Override
            public long broadcastTimeout()
            {
                return config.get( ClusterSettings.broadcast_timeout );
            }

            @Override
            public long paxosTimeout()
            {
                return config.get( ClusterSettings.paxos_timeout );
            }

            @Override
            public long phase1Timeout()
            {
                return config.get( ClusterSettings.phase1_timeout );
            }

            @Override
            public long phase2Timeout()
            {
                return config.get( ClusterSettings.phase2_timeout );
            }

            @Override
            public long learnTimeout()
            {
                return config.get( ClusterSettings.learn_timeout );
            }
        };
    }

    private final LifeSupport life = new LifeSupport();
    private final Cluster cluster;
    private final AtomicBroadcast broadcast;
    private final Heartbeat heartbeat;
    private final Snapshot snapshot;

    private final ProtocolServer server;
    private final List<URI> uris = new ArrayList<URI>(  );

    public ClusterClient( final Configuration config, final Logging logging,
                          ElectionCredentialsProvider electionCredentialsProvider )
    {
        MessageTimeoutStrategy timeoutStrategy = new MessageTimeoutStrategy(
                new FixedTimeoutStrategy( config.defaultTimeout() ) )
                .timeout( HeartbeatMessage.sendHeartbeat, config.heartbeatInterval() )
                .timeout( HeartbeatMessage.timed_out, config.heartbeatTimeout() )
                .timeout( AtomicBroadcastMessage.broadcastTimeout, config.broadcastTimeout() )
                .timeout( LearnerMessage.learnTimedout, config.learnTimeout() )
                .timeout( ProposerMessage.phase1Timeout, config.phase1Timeout() )
                .timeout( ProposerMessage.phase2Timeout, config.phase2Timeout() )
                .timeout( ClusterMessage.joiningTimeout, config.joinTimeout() )
                .timeout( ClusterMessage.configurationTimeout, config.configurationTimeout() )
                .timeout( ClusterMessage.leaveTimedout, config.leaveTimeout() )
                .timeout( ElectionMessage.electionTimeout, config.electionTimeout() );

        MultiPaxosServerFactory protocolServerFactory = new MultiPaxosServerFactory( new ClusterConfiguration( config
                .getClusterName() ), logging );

        InMemoryAcceptorInstanceStore acceptorInstanceStore = new InMemoryAcceptorInstanceStore();

        NetworkInstance networkNodeTCP = new NetworkInstance( new NetworkInstance.Configuration()
        {
            @Override
            public HostnamePort clusterServer()
            {
                return config.getAddress();
            }
        }, StringLogger.SYSTEM );

        server = life.add( protocolServerFactory.newProtocolServer( timeoutStrategy, networkNodeTCP, networkNodeTCP,
                acceptorInstanceStore, electionCredentialsProvider ) );

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
                return config.clusterDiscoveryEnabled();
            }

            @Override
            public List<HostnamePort> getInitialHosts()
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

            @Override
            public boolean isAllowedToCreateCluster()
            {
                return config.isAllowedToCreateCluster();
            }
        }, server, logging ) );

        cluster = server.newClient( Cluster.class );
        broadcast = server.newClient( AtomicBroadcast.class );
        heartbeat = server.newClient( Heartbeat.class );
        snapshot = server.newClient( Snapshot.class );
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

    public void addURI( URI addedUri )
    {
        // Remove existing URI with same scheme
        for ( URI uri : uris )
        {
            if (uri.getScheme().equals( addedUri.getScheme() ))
            {
                uris.remove( uri );
                break;
            }
        }

        uris.add( addedUri );
    }

    public Iterable<URI> getURIs()
    {
        return uris;
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
    public void setSnapshotProvider( SnapshotProvider snapshotProvider )
    {
        snapshot.setSnapshotProvider( snapshotProvider );
    }

    @Override
    public void refreshSnapshot()
    {
        snapshot.refreshSnapshot();
    }

    public void addBindingListener( BindingListener bindingListener )
    {
        server.addBindingListener( bindingListener );
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
