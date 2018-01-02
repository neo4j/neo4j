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
package org.neo4j.cluster.member.paxos;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.Heartbeat;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.function.Predicates.in;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterables.filter;

/**
 * Paxos based implementation of {@link org.neo4j.cluster.member.ClusterMemberEvents}
 */
public class PaxosClusterMemberEvents implements ClusterMemberEvents, Lifecycle
{
    private Cluster cluster;
    private AtomicBroadcast atomicBroadcast;
    private Log log;
    protected AtomicBroadcastSerializer serializer;
    protected final Listeners<ClusterMemberListener> listeners = new Listeners<>();
    private ClusterMembersSnapshot clusterMembersSnapshot;
    private ClusterListener.Adapter clusterListener;
    private Snapshot snapshot;
    private AtomicBroadcastListener atomicBroadcastListener;
    private ExecutorService executor;
    private final Predicate<ClusterMembersSnapshot> snapshotValidator;
    private final Heartbeat heartbeat;
    private HeartbeatListenerImpl heartbeatListener;
    private ObjectInputStreamFactory lenientObjectInputStream;
    private ObjectOutputStreamFactory lenientObjectOutputStream;
    private final NamedThreadFactory.Monitor namedThreadFactoryMonitor;

    public PaxosClusterMemberEvents( final Snapshot snapshot, Cluster cluster, Heartbeat heartbeat,
                                    AtomicBroadcast atomicBroadcast, LogProvider logProvider,
                                    Predicate<ClusterMembersSnapshot> validator,
                                    BiFunction<Iterable<MemberIsAvailable>, MemberIsAvailable,
                                    Iterable<MemberIsAvailable>> snapshotFilter,
                                    ObjectInputStreamFactory lenientObjectInputStream,
                                    ObjectOutputStreamFactory lenientObjectOutputStream,
                                    NamedThreadFactory.Monitor namedThreadFactoryMonitor )
    {
        this.snapshot = snapshot;
        this.cluster = cluster;
        this.heartbeat = heartbeat;
        this.atomicBroadcast = atomicBroadcast;
        this.lenientObjectInputStream = lenientObjectInputStream;
        this.lenientObjectOutputStream = lenientObjectOutputStream;
        this.namedThreadFactoryMonitor = namedThreadFactoryMonitor;
        this.log = logProvider.getLog( getClass() );

        clusterListener = new ClusterListenerImpl();

        atomicBroadcastListener = new AtomicBroadcastListenerImpl();

        this.snapshotValidator = validator;

        clusterMembersSnapshot = new ClusterMembersSnapshot( snapshotFilter );
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        listeners.add( listener );
    }

    @Override
    public void removeClusterMemberListener( ClusterMemberListener listener )
    {
        listeners.remove( listener );
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer( lenientObjectInputStream, lenientObjectOutputStream );

        cluster.addClusterListener( clusterListener );

        atomicBroadcast.addAtomicBroadcastListener( atomicBroadcastListener );

        snapshot.setSnapshotProvider( new HighAvailabilitySnapshotProvider() );

        heartbeat.addHeartbeatListener( heartbeatListener = new HeartbeatListenerImpl() );

        executor = Executors.newSingleThreadExecutor(
                new NamedThreadFactory( "Paxos event notification", namedThreadFactoryMonitor ) );
    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        snapshot.setSnapshotProvider( null );

        if ( executor != null )
        {
            executor.shutdown();
            executor = null;
        }

        cluster.removeClusterListener( clusterListener );

        atomicBroadcast.removeAtomicBroadcastListener( atomicBroadcastListener );

        heartbeat.removeHeartbeatListener( heartbeatListener );
    }

    private class HighAvailabilitySnapshotProvider implements SnapshotProvider
    {
        @Override
        public void getState( ObjectOutputStream output ) throws IOException
        {
            output.writeObject( clusterMembersSnapshot );
        }

        @Override
        public void setState( ObjectInputStream input ) throws IOException, ClassNotFoundException
        {
            clusterMembersSnapshot = ClusterMembersSnapshot.class.cast( input.readObject() );

            if ( !snapshotValidator.test( clusterMembersSnapshot ) )
            {
                executor.submit( () -> cluster.leave() );
            }
            else
            {
                // Send current availability events to listeners
                listeners.notify( executor, listener ->
                {
                    for ( MemberIsAvailable memberIsAvailable : clusterMembersSnapshot.getCurrentAvailableMembers() )
                    {
                        listener.memberIsAvailable( memberIsAvailable.getRole(), memberIsAvailable.getInstanceId(),
                                memberIsAvailable.getRoleUri(), memberIsAvailable.getStoreId() );
                    }
                } );
            }
        }
    }

    public static class UniqueRoleFilter
            implements BiFunction<Iterable<MemberIsAvailable>,MemberIsAvailable,Iterable<MemberIsAvailable>>
    {
        @Override
        public Iterable<MemberIsAvailable> apply( final Iterable<MemberIsAvailable> previousSnapshot,
                                                  final MemberIsAvailable newMessage )
        {
            return Iterables.append( newMessage, Iterables.filter( item ->
            {
                return in( newMessage.getInstanceId() ).negate().test( item.getInstanceId() );
            }, previousSnapshot ) );
        }
    }

    public static class ClusterMembersSnapshot
            implements Serializable
    {
        private static final long serialVersionUID = -4638991834604077187L;

        private BiFunction<Iterable<MemberIsAvailable>, MemberIsAvailable, Iterable<MemberIsAvailable>> nextSnapshotFunction;

        private Iterable<MemberIsAvailable> availableMembers = new ArrayList<>();

        public ClusterMembersSnapshot( BiFunction<Iterable<MemberIsAvailable>, MemberIsAvailable,
                Iterable<MemberIsAvailable>> nextSnapshotFunction )
        {
            this.nextSnapshotFunction = nextSnapshotFunction;
        }

        public void availableMember( MemberIsAvailable memberIsAvailable )
        {
            availableMembers = asList( nextSnapshotFunction.apply( availableMembers, memberIsAvailable ) );
        }

        public void unavailableMember( final InstanceId member )
        {
            availableMembers = asList( filter( item -> !item.getInstanceId().equals( member ), availableMembers ) );
        }

        public void unavailableMember( final URI member, final InstanceId id, final String role )
        {
            availableMembers = asList( filter( item ->
            {
                boolean matchByUriOrId = item.getClusterUri().equals( member ) || item.getInstanceId().equals( id );
                boolean matchByRole = item.getRole().equals( role );

                return !(matchByUriOrId && matchByRole);
            }, availableMembers ) );
        }

        public Iterable<MemberIsAvailable> getCurrentAvailableMembers()
        {
            return availableMembers;
        }

        public Iterable<MemberIsAvailable> getCurrentAvailable( final InstanceId memberId )
        {
            return asList( Iterables.filter( item ->
            {
                return item.getInstanceId().equals( memberId );
            }, availableMembers ) );
        }

    }

    private class ClusterListenerImpl extends ClusterListener.Adapter
    {
        @Override
        public void enteredCluster( ClusterConfiguration clusterConfiguration )
        {
            // Catch up with elections
            for ( Map.Entry<String, InstanceId> memberRoles : clusterConfiguration.getRoles().entrySet() )
            {
                elected( memberRoles.getKey(), memberRoles.getValue(),
                        clusterConfiguration.getUriForId( memberRoles.getValue() ) );
            }
        }

        @Override
        public void elected( String role, InstanceId instanceId, URI electedMember )
        {
            if ( role.equals( ClusterConfiguration.COORDINATOR ) )
            {
                // Use the cluster coordinator as master for HA
                listeners.notify( listener -> listener.coordinatorIsElected( instanceId ) );
            }
        }

        @Override
        public void leftCluster( InstanceId instanceId, URI member )
        {
            // Notify unavailability of members
            listeners.notify( listener ->
            {
                for ( MemberIsAvailable memberIsAvailable : clusterMembersSnapshot.getCurrentAvailable( instanceId ) )
                {
                    listener.memberIsUnavailable( memberIsAvailable.getRole(), instanceId );
                }
            } );

            clusterMembersSnapshot.unavailableMember( instanceId );
        }
    }

    private class AtomicBroadcastListenerImpl implements AtomicBroadcastListener
    {
        @Override
        public void receive( Payload payload )
        {
            try
            {
                Object value = serializer.receive( payload );
                if ( value instanceof MemberIsAvailable )
                {
                    final MemberIsAvailable memberIsAvailable = (MemberIsAvailable) value;

                    // Update snapshot
                    clusterMembersSnapshot.availableMember( memberIsAvailable );

                    log.info( "Snapshot:" + clusterMembersSnapshot.getCurrentAvailableMembers() );

                    listeners.notify( listener -> listener.memberIsAvailable(
                            memberIsAvailable.getRole(), memberIsAvailable.getInstanceId(),
                            memberIsAvailable.getRoleUri(), memberIsAvailable.getStoreId() ) );
                }
                else if ( value instanceof MemberIsUnavailable )
                {
                    final MemberIsUnavailable memberIsUnavailable = (MemberIsUnavailable) value;

                    // Update snapshot
                    clusterMembersSnapshot.unavailableMember(
                            memberIsUnavailable.getClusterUri(),
                            memberIsUnavailable.getInstanceId(),
                            memberIsUnavailable.getRole() );

                    listeners.notify( listener -> listener.memberIsUnavailable( memberIsUnavailable.getRole(),
                            memberIsUnavailable.getInstanceId() ) );
                }
            }
            catch ( Throwable t )
            {
                log.error( String.format( "Could not handle cluster member available message: %s (%d)",
                        Base64.getEncoder().encodeToString( payload.getBuf() ), payload.getLen() ), t );
            }
        }
    }

    private class HeartbeatListenerImpl implements HeartbeatListener
    {
        @Override
        public void failed( InstanceId server )
        {
            listeners.notify( listener -> listener.memberIsFailed( server ) );
        }

        @Override
        public void alive( InstanceId server )
        {
            listeners.notify( listener -> listener.memberIsAlive( server ) );
        }
    }
}
