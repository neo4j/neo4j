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
package org.neo4j.causalclustering.core.consensus.roles;

import java.io.IOException;
import java.util.Set;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.state.ReadableRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.logging.Log;

public class Election
{
    public static boolean start( ReadableRaftState ctx, Outcome outcome, Log log ) throws IOException
    {
        Set<MemberId> currentMembers = ctx.votingMembers();
        if ( currentMembers == null || !currentMembers.contains( ctx.myself() ) )
        {
            log.info( "Election attempted but not started, current members are %s, I am %s",
                    currentMembers, ctx.myself()  );
            return false;
        }

        outcome.setNextTerm( ctx.term() + 1 );

        RaftMessages.Vote.Request voteForMe =
                new RaftMessages.Vote.Request( ctx.myself(), outcome.getTerm(), ctx.myself(), ctx.entryLog()
                        .appendIndex(), ctx.entryLog().readEntryTerm( ctx.entryLog().appendIndex() ) );

        currentMembers.stream().filter( member -> !member.equals( ctx.myself() ) ).forEach( member ->
            outcome.addOutgoingMessage( new RaftMessages.Directed( member, voteForMe ) )
        );

        outcome.setVotedFor( ctx.myself() );
        log.info( "Election started with vote request: %s and members: %s", voteForMe, currentMembers );
        return true;
    }
}
