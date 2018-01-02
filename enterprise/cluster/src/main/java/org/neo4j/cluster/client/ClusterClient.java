/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.concurrent.Future;

import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.ProtocolServer;
import org.neo4j.cluster.StateMachines;
import org.neo4j.cluster.com.BindingNotifier;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.election.Election;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.cluster.statemachine.StateMachine;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.lifecycle.LifeSupport;

/**
 * These are used as clients for a Neo4j cluster. From here you can perform cluster management operations, like joining
 * and leaving clusters, as well as adding listeners for cluster events such as elections and heartbeart failures.
 * <p/>
 * Instances of this class mainly acts as a facade for the internal distributed state machines, represented by the
 * individual
 * interfaces implemented here. See the respective interfaces it implements for details on operations.
 * <p/>
 * To create one you should use the {@link ClusterClientModule}.
 */
public class ClusterClient
        implements ClusterMonitor, Cluster, AtomicBroadcast, Snapshot, Election, BindingNotifier
{
    private final Cluster cluster;
    private final AtomicBroadcast broadcast;
    private final Heartbeat heartbeat;
    private final Snapshot snapshot;
    private final Election election;
    private LifeSupport life;
    private ProtocolServer protocolServer;



    public ClusterClient( LifeSupport life, ProtocolServer protocolServer )
    {
        this.life = life;
        this.protocolServer = protocolServer;

        cluster = protocolServer.newClient( Cluster.class );
        broadcast = protocolServer.newClient( AtomicBroadcast.class );
        heartbeat = protocolServer.newClient( Heartbeat.class );
        snapshot = protocolServer.newClient( Snapshot.class );
        election = protocolServer.newClient( Election.class );
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
    public Future<ClusterConfiguration> join( String clusterName, URI... otherServerUrls )
    {
        return cluster.join( clusterName, otherServerUrls );
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
    public void demote( InstanceId node )
    {
        election.demote( node );
    }

    @Override
    public void performRoleElections()
    {
        election.performRoleElections();
    }

    @Override
    public void promote( InstanceId node, String role )
    {
        election.promote( node, role );
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
        protocolServer.addBindingListener( bindingListener );
    }

    @Override
    public void removeBindingListener( BindingListener listener )
    {
        protocolServer.removeBindingListener( listener );
    }

    public void dumpDiagnostics( StringBuilder appendTo )
    {
        StateMachines stateMachines = protocolServer.getStateMachines();
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

    public InstanceId getServerId()
    {
        return protocolServer.getServerId();
    }

    public URI getClusterServer()
    {
        return protocolServer.boundAt();
    }

    public void stop()
    {
        life.stop();
    }
}
