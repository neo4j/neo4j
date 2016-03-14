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
package org.neo4j.coreedge.raft.log;

import org.junit.Test;

import org.neo4j.coreedge.raft.ReplicatedString;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;
import static org.neo4j.coreedge.raft.log.RaftLogHelper.hasNoContent;
import static org.neo4j.coreedge.raft.log.RaftLogHelper.readLogEntry;

public abstract class RaftLogContractTest
{
    public abstract RaftLog createRaftLog() throws Exception;

    @Test
    public void shouldReportCorrectDefaultValuesOnEmptyLog() throws Exception
    {
        // given
        ReadableRaftLog log = createRaftLog();

        // then
        assertThat( log.appendIndex(), is( -1L ) );
        assertThat( log.prevIndex(), is( -1L ) );
        assertThat( log.commitIndex(), is( -1L ) );
        assertThat( log.readEntryTerm( 0 ), is( -1L ) );
        assertThat( log.readEntryTerm( -1 ), is( -1L ) );
    }

    @Test
    public void shouldResetHighTermOnTruncate() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 45, valueOf( 99 ) ) );
        log.append( new RaftLogEntry( 46, valueOf( 99 ) ) );
        log.append( new RaftLogEntry( 47, valueOf( 99 ) ) );

        // truncate the last 2
        log.truncate( 1 );

        // then
        log.append( new RaftLogEntry( 46, valueOf( 9999 ) ) );

        assertThat( log.readEntryTerm( 1 ), is( 46L ) );
        assertThat( log.appendIndex(), is( 1L ) );
    }

    @Test
    public void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntry = new RaftLogEntry( 1, valueOf( 1 ) );
        log.append( logEntry );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( log.commitIndex(), is( -1L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntry ) );
    }

    @Test
    public void shouldNotCommitWhenNoAppendedData() throws Exception
    {
        RaftLog log = createRaftLog();

        log.commit( 10 );

        assertThat( log.appendIndex(), is( -1L ) );
        assertThat( log.commitIndex(), is( -1L ) );
    }

    @Test
    public void shouldCommitOutOfOrderAppend() throws Exception
    {
        RaftLog log = createRaftLog();

        log.commit( 10 );

        RaftLogEntry logEntry = new RaftLogEntry( 1, valueOf( 1 ) );
        log.append( logEntry );

        log.commit( 10 );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( log.commitIndex(), is( 0L ) );
    }

    @Test
    public void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        log.truncate( 1 );

        assertThat( log.appendIndex(), is( 0L ) );
    }

    @Test
    public void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );
        RaftLogEntry logEntryC = new RaftLogEntry( 1, valueOf( 3 ) );
        RaftLogEntry logEntryD = new RaftLogEntry( 1, valueOf( 4 ) );
        RaftLogEntry logEntryE = new RaftLogEntry( 1, valueOf( 5 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.append( logEntryC );

        log.truncate( 1 );

        log.append( logEntryD );
        log.append( logEntryE );

        assertThat( log.appendIndex(), is( 2L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryD ) );
        assertThat( readLogEntry( log, 2 ), equalTo( logEntryE ) );
    }

    @Test
    public void shouldHaveNoEffectWhenTruncatingNonExistingEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );

        log.truncate( 5 );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryB ) );
    }

    @Test
    public void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        assertThat( readLogEntry( log, 0 ), equalTo( logEntryA ) );
        assertThat( readLogEntry( log, 1 ), equalTo( logEntryB ) );
    }

    @Test
    public void shouldRejectNonMonotonicTermsForEntries() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        try
        {
            // when the term has a lower value
            log.append( new RaftLogEntry( 0, valueOf( 3 ) ) );
            // then an exception should be thrown
            fail( "Should have failed because of non-monotonic terms" );
        }
        catch ( IllegalStateException expected )
        {
            // expected
        }
    }

    @Test
    public void shouldCommitAndThenTruncateSubsequentEntry() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        // when
        log.commit( toCommit );
        log.truncate( toTruncate );

        // then
        assertThat( log.appendIndex(), is( toCommit ) );
        assertThat( log.readEntryTerm( toCommit ), is( 0L ) );
    }

    @Test
    public void shouldTruncateAndThenCommitPreviousEntry() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        // when
        log.truncate( toTruncate );
        log.commit( toCommit );

        // then
        assertThat( log.appendIndex(), is( toCommit ) );
        assertThat( log.readEntryTerm( toCommit ), is( 0L ) );
    }

    @Test
    public void shouldCommitAfterTruncatingAndAppending() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        /*
          0 1 2 Tr(2) 2 C*(1)
         */

        // when
        log.truncate( toTruncate );
        long lastAppended = log.append( new RaftLogEntry( 2, valueOf( 3 ) ) );
        log.commit( toCommit );

        // then
        assertThat( log.appendIndex(), is( lastAppended ) );
        assertThat( log.readEntryTerm( toCommit ), is( 0L ) );
        assertThat( log.readEntryTerm( lastAppended ), is( 2L ) );
    }

    @Test
    public void shouldCommitAfterAppendingAndTruncating() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toTruncate = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        // when
        log.truncate( toTruncate );
        log.commit( toCommit );

        // then
        assertThat( log.appendIndex(), is( toCommit ) );
        assertThat( log.readEntryTerm( toCommit ), is( 0L ) );
    }

    @Test
    public void shouldNotAllowTruncationAtLastCommit() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toCommit = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        log.commit( toCommit );

        try
        {
            // when
            log.truncate( toCommit );
            fail( "Truncation at this point should have failed" );
        }
        catch ( IllegalArgumentException truncationFailed )
        {
            // awesome
        }

        // then
        assertThat( log.appendIndex(), is( toCommit ) );
    }

    @Test
    public void shouldNotAllowTruncationBeforeLastCommit() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 0, valueOf( 0 ) ) );
        long toTryToTruncate = log.append( new RaftLogEntry( 0, valueOf( 1 ) ) );
        long toCommit = log.append( new RaftLogEntry( 1, valueOf( 2 ) ) );

        log.commit( toCommit );

        try
        {
            // when
            log.truncate( toTryToTruncate );
            fail( "Truncation at this point should have failed" );
        }
        catch ( IllegalArgumentException truncationFailed )
        {
            // awesome
        }

        // then
        assertThat( log.appendIndex(), is( toCommit ) );
    }

    @Test
    public void shouldEventuallyPrune() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        int term = 0;

        long safeIndex = -1;
        long prunedIndex = -1;

        // this loop should eventually be able to prune something
        while ( prunedIndex == -1 )
        {
            for ( int i = 0; i < 100; i++ )
            {
                log.append( new RaftLogEntry( term, valueOf( 10 * term ) ) );
                term++;
            }
            safeIndex = log.appendIndex() - 50;
            // when
            prunedIndex = log.prune( safeIndex );
        }

        // then
        assertThat( prunedIndex, lessThanOrEqualTo( safeIndex ) );
        assertEquals( prunedIndex, log.prevIndex() );
        assertEquals( prunedIndex, log.readEntryTerm( prunedIndex ) );

        final long[] expectedVal = {prunedIndex + 1};
        log.getEntryCursor( prunedIndex + 1 )
                .forAll( ( entry ) -> assertThat( entry.content(), is( valueOf( 10*(int)expectedVal[0]++ ) ) ) );

        assertThat( log, hasNoContent( prunedIndex ) );
    }

    @Test
    public void shouldSkipAheadInEmptyLog() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        // when
        long skipIndex = 10;
        long skipTerm = 2;
        log.skip( skipIndex, skipTerm );

        // then
        assertEquals( skipIndex, log.appendIndex() );
        assertEquals( skipTerm, log.readEntryTerm( skipIndex ) );
    }

    @Test
    public void shouldSkipAheadInLogWithContent() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        long term = 0;
        int entryCount = 5;
        for ( int i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }

        // when
        long skipIndex = entryCount + 5;
        long skipTerm = term + 2;
        log.skip( skipIndex, skipTerm );

        // then
        assertEquals( skipIndex, log.appendIndex() );
        assertEquals( skipTerm, log.readEntryTerm( skipIndex ) );
    }

    @Test
    public void shouldNotSkipInLogWithLaterContent() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        long term = 0;
        int entryCount = 5;
        for ( int i = 0; i < entryCount; i++ )
        {
            log.append( new RaftLogEntry( term, valueOf( i ) ) );
        }
        long lastIndex = log.appendIndex();

        // when
        long skipIndex = entryCount - 2;
        log.skip( skipIndex, term );

        // then
        assertEquals( lastIndex, log.appendIndex() );
        assertEquals( term, log.readEntryTerm( skipIndex ) );
    }

    @Test
    public void shouldBeAbleToAppendAfterSkipping() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        // when
        long skipIndex = 5;
        long term = 0;
        log.skip( skipIndex, term );

        int newContentValue = 100;
        long newEntryIndex = skipIndex + 1;
        long appendedIndex = log.append( new RaftLogEntry( term, valueOf( newContentValue ) ) );

        // then
        assertEquals( newEntryIndex, log.appendIndex() );
        assertEquals( newEntryIndex, appendedIndex );

        try
        {
            readLogEntry( log, skipIndex );
            fail( "Should have thrown exception" );
        }
        catch ( RaftLogCompactedException e )
        {
            // expected
        }
        assertThat( readLogEntry( log, newEntryIndex ).content(), is( valueOf( newContentValue ) ) );
    }
}
