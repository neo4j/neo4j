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
