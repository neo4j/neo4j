/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
    public void goalAchievedWhenCatchupRoundDurationLessThanTarget() throws Exception
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
        public long readEntryTerm( long logIndex ) throws IOException
        {
            return 0;
        }

        @Override
        public RaftLogCursor getEntryCursor( long fromIndex ) throws IOException
        {
            return RaftLogCursor.empty();
        }
    }
}
