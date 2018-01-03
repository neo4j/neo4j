/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.membership;

import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;

class CatchupGoalTracker
{
    static final long MAX_ROUNDS = 10;

    private final ReadableRaftLog raftLog;
    private final Clock clock;

    private long startTime;
    private  long roundStartTime;
    private final long roundTimeout;
    private long roundCount;
    private long catchupTimeout;

    private long targetIndex;
    private boolean finished;
    private boolean goalAchieved;

    CatchupGoalTracker( ReadableRaftLog raftLog, Clock clock, long roundTimeout, long catchupTimeout )
    {
        this.raftLog = raftLog;
        this.clock = clock;
        this.roundTimeout = roundTimeout;
        this.catchupTimeout = catchupTimeout;
        this.targetIndex = raftLog.appendIndex();
        this.startTime = clock.millis();
        this.roundStartTime = clock.millis();

        this.roundCount = 1;
    }

    void updateProgress( FollowerState followerState )
    {
        if ( finished )
        {
            return;
        }

        boolean achievedTarget = followerState.getMatchIndex() >= targetIndex;
        if ( achievedTarget && (clock.millis() - roundStartTime) <= roundTimeout )
        {
            goalAchieved = true;
            finished = true;
        }
        else if ( clock.millis() > (startTime + catchupTimeout) )
        {
            finished = true;
        }
        else if ( achievedTarget )
        {
            if ( roundCount < MAX_ROUNDS )
            {
                roundCount++;
                roundStartTime = clock.millis();
                targetIndex = raftLog.appendIndex();
            }
            else
            {
                finished = true;
            }
        }
    }

    boolean isFinished()
    {
        return finished;
    }

    boolean isGoalAchieved()
    {
        return goalAchieved;
    }

    @Override
    public String toString()
    {
        return String.format( "CatchupGoalTracker{startTime=%d, roundStartTime=%d, roundTimeout=%d, roundCount=%d, " +
                "catchupTimeout=%d, targetIndex=%d, finished=%s, goalAchieved=%s}", startTime, roundStartTime,
                roundTimeout, roundCount, catchupTimeout, targetIndex, finished, goalAchieved );
    }
}
