/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.state.membership;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.coreedge.raft.membership.RaftMembership;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.coreedge.raft.state.StateMarshal;
import org.neo4j.storageengine.api.ReadPastEndException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftMembershipState<MEMBER> implements RaftMembership<MEMBER>
{
    private Set<MEMBER> additionalReplicationMembers = new HashSet<>();

    private volatile Set<MEMBER> votingMembers = new HashSet<>();
    private volatile Set<MEMBER> replicationMembers = new HashSet<>(); // votingMembers + additionalReplicationMembers

    private final Set<Listener> listeners;

    private long logIndex = -1; // First log index is 0, so -1 is used here as "unknown" value

    private RaftMembershipState( Set<MEMBER> members, long logIndex )
    {
        this.votingMembers = members;
        this.logIndex = logIndex;
        this.listeners = new HashSet<>(  );
        updateReplicationMembers();
    }

    public RaftMembershipState()
    {
        this.listeners = new HashSet<>();
    }

    public synchronized void setVotingMembers( Set<MEMBER> newVotingMembers )
    {
        this.votingMembers = new HashSet<>( newVotingMembers );

        updateReplicationMembers();
        notifyListeners();
    }

    public synchronized void addAdditionalReplicationMember( MEMBER member )
    {
        additionalReplicationMembers.add( member );

        updateReplicationMembers();
        notifyListeners();
    }

    public synchronized void removeAdditionalReplicationMember( MEMBER member )
    {
        additionalReplicationMembers.remove( member );

        updateReplicationMembers();
        notifyListeners();
    }

    public void logIndex( long logIndex )
    {
        this.logIndex = logIndex;
    }

    private void updateReplicationMembers()
    {
        HashSet<MEMBER> newReplicationMembers = new HashSet<>( votingMembers );

        newReplicationMembers.addAll( additionalReplicationMembers );
        this.replicationMembers = newReplicationMembers;
    }

    @Override
    public Set<MEMBER> votingMembers()
    {
        return new HashSet<>( votingMembers );
    }

    @Override
    public Set<MEMBER> replicationMembers()
    {
        return new HashSet<>( replicationMembers );
    }

    @Override
    public long logIndex()
    {
        return logIndex;
    }

    @Override
    public synchronized void registerListener( Listener listener )
    {
        listeners.add( listener );
    }

    @Override
    public synchronized void deregisterListener( Listener listener )
    {
        listeners.remove( listener );
    }

    private void notifyListeners()
    {
        listeners.forEach( Listener::onMembershipChanged );
    }

    public static class Marshal<MEMBER> implements StateMarshal<RaftMembershipState<MEMBER>>
    {
        private final ChannelMarshal<MEMBER> memberMarshal;

        public Marshal( ChannelMarshal<MEMBER> marshal )
        {
            this.memberMarshal = marshal;
        }

        @Override
        public void marshal( RaftMembershipState<MEMBER> state, WritableChannel channel ) throws IOException
        {
            channel.putLong( state.logIndex );
            channel.putInt( state.votingMembers.size() );
            for ( MEMBER votingMember : state.votingMembers )
            {
                memberMarshal.marshal( votingMember, channel );
            }
        }

        @Override
        public RaftMembershipState<MEMBER> unmarshal( ReadableChannel source ) throws IOException
        {
            try
            {
                long logIndex = source.getLong();
                int memberCount = source.getInt();
                Set<MEMBER> members = new HashSet<>();
                for ( int i = 0; i < memberCount; i++ )
                {
                    members.add( memberMarshal.unmarshal( source ) );
                }
                return new RaftMembershipState<>( members, logIndex );
            }
            catch ( ReadPastEndException noMoreBytes )
            {
                return null;
            }
        }

        @Override
        public RaftMembershipState<MEMBER> startState()
        {
            return new RaftMembershipState<>();
        }

        @Override
        public long ordinal( RaftMembershipState<MEMBER> state )
        {
            return state.logIndex();
        }
    }
}
