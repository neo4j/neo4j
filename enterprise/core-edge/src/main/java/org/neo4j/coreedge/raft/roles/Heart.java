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
package org.neo4j.coreedge.raft.roles;

import org.neo4j.coreedge.raft.RaftMessages;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.outcome.Outcome;
import org.neo4j.coreedge.raft.state.ReadableRaftState;

public class Heart
{
    public static <MEMBER> void beat( ReadableRaftState<MEMBER> state, Outcome<MEMBER> outcome, RaftMessages.Heartbeat<MEMBER> request ) throws RaftStorageException
    {
        if ( request.leaderTerm() < state.term() )
        {
            return;
        }

        outcome.renewElectionTimeout();
        outcome.setNextTerm( request.leaderTerm() );
        outcome.setLeader( request.from() );
        outcome.setLeaderCommit( request.commitIndex() );

        if ( !Follower.logHistoryMatches( state, request.commitIndex(), request.commitIndexTerm() ) )
        {
            return;
        }

        Follower.commitToLogOnUpdate( state, request.commitIndex(), request.commitIndex(), outcome );
    }
}
