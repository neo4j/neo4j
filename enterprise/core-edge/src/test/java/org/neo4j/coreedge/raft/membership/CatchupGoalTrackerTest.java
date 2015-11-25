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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.state.FollowerState;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.helpers.FakeClock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CatchupGoalTrackerTest
{

    public static final long ROUND_TIMEOUT = 15;
    public static final long CATCHUP_TIMEOUT = 1_000;

    @Test
    public void shouldAchieveGoalIfWithinRoundTimeout() throws Exception
    {
        FakeClock clock = new FakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT - 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        assertTrue( catchupGoalTracker.isGoalAchieved() );
        assertTrue( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldNotAchieveGoalIfBeyondRoundTimeout() throws Exception
    {
        FakeClock clock = new FakeClock();
        StubLog log = new StubLog();

        log.setAppendIndex( 10 );
        CatchupGoalTracker catchupGoalTracker = new CatchupGoalTracker( log, clock, ROUND_TIMEOUT, CATCHUP_TIMEOUT );

        clock.forward( ROUND_TIMEOUT + 5, TimeUnit.MILLISECONDS );
        catchupGoalTracker.updateProgress( new FollowerState().onSuccessResponse( 10 ) );

        assertFalse( catchupGoalTracker.isGoalAchieved() );
        assertFalse( catchupGoalTracker.isFinished() );
    }

    @Test
    public void shouldFailToAchieveGoalDueToCatchupTimeoutExpiring() throws Exception
    {
        FakeClock clock = new FakeClock();
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
    public void shouldFailToAchieveGoalDueToCatchupTimeoutExpiringEvenThoughWeDoEventuallyAchieveTarget() throws Exception
    {
        FakeClock clock = new FakeClock();
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
    public void shouldFailToAchieveGoalDueToRoundExhaustion() throws Exception
    {
        FakeClock clock = new FakeClock();
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
    public void shouldNotFinishIfRoundsNotExhausted() throws Exception
    {
        FakeClock clock = new FakeClock();
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

        @Override public long commitIndex()
        {
            return 0;
        }

        @Override public RaftLogEntry readLogEntry( long logIndex ) throws RaftStorageException
        {
            return null;
        }

        @Override public ReplicatedContent readEntryContent( long logIndex ) throws RaftStorageException
        {
            return null;
        }

        @Override public long readEntryTerm( long logIndex ) throws RaftStorageException
        {
            return 0;
        }

        @Override public boolean entryExists( long logIndex )
        {
            return false;
        }
    }
}
