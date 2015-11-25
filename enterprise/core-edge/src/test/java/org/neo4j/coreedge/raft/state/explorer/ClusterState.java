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
package org.neo4j.coreedge.raft.state.explorer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.raft.state.RaftState;

import static org.neo4j.coreedge.raft.state.RaftStateBuilder.raftState;

public class ClusterState
{
    public final Map<RaftTestMember, Role> roles;
    public final Map<RaftTestMember, ComparableRaftState> states;
    public final Map<RaftTestMember, Queue<RaftMessages.Message<RaftTestMember>>> queues;

    public ClusterState( Set<RaftTestMember> members ) throws RaftStorageException
    {
        this.roles = new HashMap<>();
        this.states = new HashMap<>();
        this.queues = new HashMap<>();

        for ( RaftTestMember member : members )
        {
            roles.put( member, Role.FOLLOWER );
            RaftState<RaftTestMember> memberState = raftState().myself( member ).votingMembers( members ).build();
            states.put( member, new ComparableRaftState( memberState ) );
            queues.put( member, new LinkedList<RaftMessages.Message<RaftTestMember>>() );
        }
    }

    public ClusterState( ClusterState original )
    {
        this.roles = new HashMap<>( original.roles );
        this.states = new HashMap<>( original.states );
        this.queues = new HashMap<>( original.queues );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ClusterState that = (ClusterState) o;
        return Objects.equals( roles, that.roles ) &&
                Objects.equals( states, that.states ) &&
                Objects.equals( queues, that.queues );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( roles, states, queues );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( RaftTestMember member : roles.keySet() )
        {
            builder.append( member ).append( " : " ).append( roles.get( member ) ).append( "\n" );
            builder.append( "  state: " ).append( states.get( member ) ).append( "\n" );
            builder.append( "  queue: " ).append( queues.get( member ) ).append( "\n" );
        }
        return builder.toString();
    }
}
