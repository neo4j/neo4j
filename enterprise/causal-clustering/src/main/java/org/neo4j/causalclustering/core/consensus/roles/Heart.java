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
package org.neo4j.causalclustering.core.consensus.roles;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.logging.Log;

class Heart
{
    private Heart()
    {
    }

    static void beat( ReadableRaftState state, Outcome outcome, RaftMessages.Heartbeat request, Log log )
            throws IOException
    {
        if ( request.leaderTerm() < state.term() )
        {
            return;
        }

        outcome.setPreElection( false );
        outcome.setNextTerm( request.leaderTerm() );
        outcome.setLeader( request.from() );
        outcome.setLeaderCommit( request.commitIndex() );
        outcome.addOutgoingMessage( new RaftMessages.Directed( request.from(),
                new RaftMessages.HeartbeatResponse( state.myself() ) ) );

        if ( !Follower.logHistoryMatches( state, request.commitIndex(), request.commitIndexTerm() ) )
        {
            return;
        }

        Follower.commitToLogOnUpdate( state, request.commitIndex(), request.commitIndex(), outcome );
    }
}
