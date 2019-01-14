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
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.roles.follower.FollowerState;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CatchupGoalTrackerTest
{

    private static final long ROUND_TIMEOUT = 15;
    private static final long CATCHUP_TIMEOUT = 1_000;

    @Test
    public void shouldAchieveGoalIfWithinRoundTimeout()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT - 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        assertTrue( catchupGoalTracker.isGoalAchieved() );
        assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldNotAchieveGoalIfBeyondRoundTimeout()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT + 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertFalse( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldFailToAchieveGoalDueToCatchupTimeoutExpiring()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        // when
        clock.forward( CATCHUP_TIMEOUT + 10, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 4 ) );

        // then
        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldFailToAchieveGoalDueToCatchupTimeoutExpiringEvenThoughWeDoEventuallyAchieveTarget()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        // when
        clock.forward( CATCHUP_TIMEOUT + 10, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        // then
        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldFailToAchieveGoalDueToRoundExhaustion()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        long appendIndex = 10;
        log.setAppendIndex( appendIndex );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        for ( int i = 0; i < CatchupGoalTracker.MAX_ROUNDS; i++ )
        {
            appendIndex += 10;
            log.setAppendIndex( appendIndex );
            clock.forward( ROUND_TIMEOUT + 1, TimeUnit.MILLISECONDS );
            catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( appendIndex ) );
        }

        // then
        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldNotFinishIfRoundsNotExhausted()
    {
        FakeClock clock = Clocks.fakeClock();
        StubLog log = new StubLog();

        long appendIndex = 10;
        log.setAppendIndex( appendIndex );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        for ( int i = 0; i < CatchupGoalTracker.MAX_ROUNDS - 5; i++ )
        {
            appendIndex += 10;
            log.setAppendIndex( appendIndex );
            clock.forward( ROUND_TIMEOUT + 1, TimeUnit.MILLISECONDS );
            catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( appendIndex ) );
        }

        // then
        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertFalse( catchupGoalTracker.isFinished() );
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
