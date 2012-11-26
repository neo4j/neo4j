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

package org.neo4j.cluster.member.paxos;

import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.toList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.cluster.member.ClusterMemberEvents;
import org.neo4j.cluster.member.ClusterMemberListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcast;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastListener;
import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.Payload;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;
import org.neo4j.helpers.Listeners;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Paxos based implementation of {@link org.neo4j.cluster.member.ClusterMemberEvents}
 */
public class PaxosClusterMemberEvents implements ClusterMemberEvents, Lifecycle
{
    private Cluster cluster;
    private AtomicBroadcast atomicBroadcast;
    private StringLogger logger;
    protected AtomicBroadcastSerializer serializer;
    protected Iterable<ClusterMemberListener> listeners = Listeners.newListeners();
    private ClusterMembersSnapshot clusterMembersSnapshot = new ClusterMembersSnapshot();
    private ClusterListener.Adapter clusterListener;
    private Snapshot snapshot;
    private AtomicBroadcastListener atomicBroadcastListener;
    private ExecutorService executor;

    public PaxosClusterMemberEvents( final Snapshot snapshot, Cluster cluster, AtomicBroadcast atomicBroadcast, StringLogger logger )
    {
        this.snapshot = snapshot;
        this.cluster = cluster;
        this.atomicBroadcast = atomicBroadcast;
        this.logger = logger;

        clusterListener = new ClusterListenerImpl();

        atomicBroadcastListener = new AtomicBroadcastListenerImpl();
    }

    @Override
    public void addClusterMemberListener( ClusterMemberListener listener )
    {
        listeners = Listeners.addListener( listener, listeners );
    }

    @Override
    public void removeClusterMemberListener( ClusterMemberListener listener )
    {
        listeners = Listeners.removeListener( listener, listeners );
    }

    @Override
    public void init()
            throws Throwable
    {
        serializer = new AtomicBroadcastSerializer();

        cluster.addClusterListener( clusterListener );

        atomicBroadcast.addAtomicBroadcastListener( atomicBroadcastListener );

        snapshot.setSnapshotProvider( new HighAvailabilitySnapshotProvider() );
    }

    @Override
    public void start()
            throws Throwable
    {
        executor = Executors.newSingleThreadExecutor();

    }

    @Override
    public void stop()
            throws Throwable
    {
        if ( executor != null )
        {
            executor.shutdown();
            executor = null;
        }
    }

    @Override
    public void shutdown()
            throws Throwable
    {
        cluster.removeClusterListener( clusterListener );

        atomicBroadcast.removeAtomicBroadcastListener( atomicBroadcastListener );

        snapshot.setSnapshotProvider( null );
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
            clusterMembersSnapshot = ClusterMembersSnapshot.class.cast(input.readObject());

            // Send current availability events to listeners
            Listeners.notifyListeners( listeners, executor, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    for ( MemberIsAvailable memberIsAvailable : clusterMembersSnapshot.getCurrentAvailableMembers() )
                    {
                        listener.memberIsAvailable( memberIsAvailable.getRole(), memberIsAvailable.getClusterUri(), memberIsAvailable.getRoleUri() );
                    }
                }
            } );
        }
    }

    public static class ClusterMembersSnapshot
        implements Serializable
    {
        Iterable<MemberIsAvailable> availableMembers = new ArrayList<MemberIsAvailable>(  );

        public void availableMember(MemberIsAvailable memberIsAvailable)
        {
            availableMembers = toList( append( memberIsAvailable, availableMembers ) );
        }

        public void unavailableMember( final URI member )
        {
            availableMembers = toList( filter( new Predicate<MemberIsAvailable>()
            {
                @Override
                public boolean accept( MemberIsAvailable item )
                {
                    return !item.getClusterUri().equals( member );
                }
            }, availableMembers ));
        }

        public void unavailableMember( final URI member, final String role )
        {
            availableMembers = toList( filter(new Predicate<MemberIsAvailable>()
            {
                @Override
                public boolean accept( MemberIsAvailable item )
                {
                    return !(item.getClusterUri().equals( member ) && item.getRole().equals( role ));
                }
            }, availableMembers));
        }

        public Iterable<MemberIsAvailable> getCurrentAvailableMembers()
        {
            return availableMembers;
        }

        public Iterable<MemberIsAvailable> getCurrentAvailable( final URI memberUri)
        {
            return Iterables.filter( new Predicate<MemberIsAvailable>()
                                    {
                                        @Override
                                        public boolean accept( MemberIsAvailable item )
                                        {
                                            return item.getClusterUri().equals( memberUri );
                                        }
                                    }, availableMembers);
        }

    }

    private class ClusterListenerImpl extends ClusterListener.Adapter
    {
        @Override
        public void enteredCluster( ClusterConfiguration clusterConfiguration )
        {
            snapshot.refreshSnapshot();

            // Catch up with elections
            for ( Map.Entry<String, URI> memberRoles : clusterConfiguration.getRoles().entrySet() )
            {
                elected( memberRoles.getKey(), memberRoles.getValue() );
            }
        }

        @Override
        public void elected( String role, final URI electedMember )
        {
            if ( role.equals( ClusterConfiguration.COORDINATOR ) )
            {
                // Use the cluster coordinator as master for HA
                Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
                {
                    @Override
                    public void notify( ClusterMemberListener listener )
                    {
                        listener.masterIsElected( electedMember );
                    }
                } );
            }
        }

        @Override
        public void leftCluster( final URI member )
        {
            // Notify unavailability of members
            Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
            {
                @Override
                public void notify( ClusterMemberListener listener )
                {
                    for ( MemberIsAvailable memberIsAvailable : clusterMembersSnapshot.getCurrentAvailable( member ) )
                    {
                        listener.memberIsUnavailable( memberIsAvailable.getRole(), member );
                    }
                }
            } );

            clusterMembersSnapshot.unavailableMember( member );
        }
    }

    private class AtomicBroadcastListenerImpl implements AtomicBroadcastListener
    {
        @Override
        public void receive( Payload payload )
        {
            try
            {
                final Object value = serializer.receive( payload );
                if ( value instanceof MemberIsAvailable )
                {
                    final MemberIsAvailable memberIsAvailable = (MemberIsAvailable) value;

                    // Update snapshot
                    clusterMembersSnapshot.availableMember( memberIsAvailable );

                    Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
                    {
                        @Override
                        public void notify( ClusterMemberListener listener )
                        {
                            listener.memberIsAvailable( memberIsAvailable.getRole(),
                                    memberIsAvailable.getClusterUri(),
                                    memberIsAvailable.getRoleUri() );
                        }
                    } );
                }
                else if ( value instanceof MemberIsUnavailable )
                {
                    final MemberIsUnavailable memberIsUnavailable = (MemberIsUnavailable) value;

                    // Update snapshot
                    clusterMembersSnapshot.unavailableMember( memberIsUnavailable.getClusterUri(),
                            memberIsUnavailable.getRole() );

                    Listeners.notifyListeners( listeners, new Listeners.Notification<ClusterMemberListener>()
                    {
                        @Override
                        public void notify( ClusterMemberListener listener )
                        {
                            listener.memberIsUnavailable( memberIsUnavailable.getRole(),
                                    memberIsUnavailable.getClusterUri() );
                        }
                    } );
                }
            }
            catch ( Throwable t )
            {
                logger.error( "Could not handle cluster member available message", t );
            }
        }
    }
}
