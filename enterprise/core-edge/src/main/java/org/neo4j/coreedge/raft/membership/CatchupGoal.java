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

import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.state.FollowerState;
import org.neo4j.helpers.Clock;

public class CatchupGoal
{
    private static final long MAX_ROUNDS = 10;

    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final long electionTimeout;

    private long targetIndex;
    private long roundCount;
    private long startTime;

    public CatchupGoal( ReadableRaftLog raftLog, Clock clock, long electionTimeout )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.electionTimeout = electionTimeout;
        this.targetIndex = raftLog.appendIndex();
        this.startTime = clock.currentTimeMillis();

        this.roundCount = 1;
    }

    boolean achieved( FollowerState followerState )
    {
        if ( followerState.getMatchIndex() >= targetIndex )
        {
            if ( (clock.currentTimeMillis() - startTime) <= electionTimeout )
            {
                return true;
            }
            else if ( roundCount <  MAX_ROUNDS )
            {
                roundCount++;
                startTime = clock.currentTimeMillis();
                targetIndex = raftLog.appendIndex();
            }
        }
        return false;
    }
}
