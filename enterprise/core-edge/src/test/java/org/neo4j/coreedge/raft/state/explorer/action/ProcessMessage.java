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
package org.neo4j.coreedge.raft.state.explorer.action;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.coreedge.raft.log.RaftLogCompactedException;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.explorer.ClusterState;
import org.neo4j.coreedge.raft.state.explorer.ComparableRaftState;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

public class ProcessMessage implements Action
{
    private final RaftTestMember member;
    private Log log = NullLog.getInstance();

    public ProcessMessage( RaftTestMember member )
    {
        this.member = member;
    }

    @Override
    public ClusterState advance( ClusterState previous ) throws IOException, RaftLogCompactedException
    {
        ClusterState newClusterState = new ClusterState( previous );
        Queue<RaftMessages.RaftMessage<RaftTestMember>> inboundQueue = new LinkedList<>( previous.queues.get( member ) );
        RaftMessages.RaftMessage<RaftTestMember> message = inboundQueue.poll();
        if ( message == null )
        {
            return previous;
        }
        ComparableRaftState memberState = previous.states.get( member );
        ComparableRaftState newMemberState = new ComparableRaftState( memberState );
        Outcome<RaftTestMember> outcome = previous.roles.get( member ).handler.handle( message, memberState, log );
        newMemberState.update( outcome );

        for ( RaftMessages.Directed<RaftTestMember> outgoingMessage : outcome.getOutgoingMessages() )
        {
            LinkedList<RaftMessages.RaftMessage<RaftTestMember>> outboundQueue =
                    new LinkedList<>( newClusterState.queues.get( outgoingMessage.to() ) );
            outboundQueue.add( outgoingMessage.message() );
            newClusterState.queues.put( outgoingMessage.to(), outboundQueue );
        }

        newClusterState.roles.put( member, outcome.getRole() );
        newClusterState.states.put( member, newMemberState );
        newClusterState.queues.put( member, inboundQueue );
        return newClusterState;
    }
}
