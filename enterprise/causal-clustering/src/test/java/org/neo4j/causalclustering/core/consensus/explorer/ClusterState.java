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
package org.neo4j.causalclustering.core.consensus.explorer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.consensus.state.RaftState;
import org.neo4j.causalclustering.identity.MemberId;

import static org.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.raftState;

public class ClusterState
{
    public final Map<MemberId, Role> roles;
    public final Map<MemberId, ComparableRaftState> states;
    public final Map<MemberId, Queue<RaftMessages.RaftMessage>> queues;

    public ClusterState( Set<MemberId> members ) throws IOException
    {
        this.roles = new HashMap<>();
        this.states = new HashMap<>();
        this.queues = new HashMap<>();

        for ( MemberId member : members )
        {
            roles.put( member, Role.FOLLOWER );
            RaftState memberState = raftState().myself( member ).votingMembers( members ).build();
            states.put( member, new ComparableRaftState( memberState ) );
            queues.put( member, new LinkedList<>() );
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
        for ( MemberId member : roles.keySet() )
        {
            builder.append( member ).append( " : " ).append( roles.get( member ) ).append( "\n" );
            builder.append( "  state: " ).append( states.get( member ) ).append( "\n" );
            builder.append( "  queue: " ).append( queues.get( member ) ).append( "\n" );
        }
        return builder.toString();
    }
}
