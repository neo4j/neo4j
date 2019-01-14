/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.membership;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CatchupGoalTest
{
    @Test
    public void goalAchievedWhenCatchupRoundDurationLessThanTarget()
    {
        FakeClock clock = Clocks.fakeClock();
        long electionTimeout = 15;
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoal goal = new CatchupGoal( log, clock, electionTimeout );

        log.setAppendIndex( 20 );
        clock.forward( 10, MILLISECONDS );
        assertFalse( goal.achieved( new FollowerState() ) );

        log.setAppendIndex( 30 );
        clock.forward( 10, MILLISECONDS );
        assertFalse( goal.achieved( new FollowerState().onSuccessResponse( 10 ) ) );

        log.setAppendIndex( 40 );
        clock.forward( 10, MILLISECONDS );
        assertTrue( goal.achieved( new FollowerState().onSuccessResponse( 30 ) ) );
    }

    private class StubLog implements ReadableRaftLog
    {
        private long appendIndex;

        private void setAppendIndex( long index )
        {
            this.appendIndex = index;
        }

        @Override
        public long appendIndex()
        {
            return appendIndex;
        }

        @Override
        public long prevIndex()
        {
            return 0;
        }

        @Override
        public long readEntryTerm( long logIndex )
        {
            return 0;
        }

        @Override
        public RaftLogCursor getEntryCursor( long fromIndex )
        {
            return RaftLogCursor.empty();
        }
    }
}
