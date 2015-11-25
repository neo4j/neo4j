/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.membership;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftMembershipImpl<MEMBER> implements RaftMembership<MEMBER>
{
    private Set<MEMBER> additionalReplicationMembers = new HashSet<>();

    private volatile Set<MEMBER> votingMembers = new HashSet<>();
    private volatile Set<MEMBER> replicationMembers = new HashSet<>(); // votingMembers + additionalReplicationMembers

    private final Set<Listener> listeners = new HashSet<>();

    private final Log log;

    public RaftMembershipImpl( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void setVotingMembers( Set<MEMBER> newVotingMembers )
    {
        this.votingMembers = new HashSet<>( newVotingMembers );

        updateReplicationMembers();
        notifyListeners();

        log.info( "Voting members: " + votingMembers );
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

    private void updateReplicationMembers()
    {
        HashSet<MEMBER> newReplicationMembers = new HashSet<>( votingMembers );

        newReplicationMembers.addAll( additionalReplicationMembers );
        this.replicationMembers = newReplicationMembers;

        log.info( "Replication members: " + newReplicationMembers );
    }

    @Override
    public Set<MEMBER> votingMembers()
    {
        return votingMembers;
    }

    @Override
    public Set<MEMBER> replicationMembers()
    {
        return replicationMembers;
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
}
