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
package org.neo4j.coreedge.core.consensus.roles;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.ReplicatedInteger;
import org.neo4j.coreedge.core.consensus.log.RaftLogEntry;
import org.neo4j.coreedge.core.consensus.log.ReadableRaftLog;
import org.neo4j.coreedge.core.consensus.outcome.RaftLogCommand;
import org.neo4j.coreedge.core.consensus.outcome.Outcome;
import org.neo4j.coreedge.core.consensus.outcome.TruncateLogCommand;
import org.neo4j.coreedge.core.consensus.state.ReadableRaftState;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.identity.RaftTestMember.member;

public class AppendingTest
{
    private MemberId aMember = member( 0 );

    @Test
    public void shouldPerformTruncation() throws Exception
    {
        // when
        // we have a log appended up to appendIndex, and committed somewhere before that
        long appendIndex = 5;
        long localTermForAllEntries = 1L;

        Outcome outcome = mock( Outcome.class );
        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( appendIndex );

        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( appendIndex - 3 );

        // when
        // the leader asks to append after the commit index an entry that mismatches on term
        Appending.handleAppendEntriesRequest( state, outcome,
                new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, appendIndex - 2,
                        localTermForAllEntries,
                        new RaftLogEntry[]{
                                new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                        appendIndex + 3 ), NullLog.getInstance() );

        // then
        // we must produce a TruncateLogCommand at the earliest mismatching index
        verify( outcome, times( 1 ) ).addLogCommand( argThat( new LogCommandMatcher( appendIndex - 1 ) ) );
    }

    @Test
    public void shouldNotAllowTruncationAtCommit() throws Exception
    {
        // given
        long commitIndex = 5;
        long localTermForAllEntries = 1L;

        Outcome outcome = mock( Outcome.class );
        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );

        // when - then
        try
        {
            Appending.handleAppendEntriesRequest( state, outcome,
                    new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, commitIndex - 1,
                            localTermForAllEntries,
                            new RaftLogEntry[]{
                                    new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                            commitIndex + 3 ), NullLog.getInstance() );
            fail( "Appending should not allow truncation at or before the commit index" );
        }
        catch ( IllegalStateException expected )
        {
            // ok
        }
    }

    @Test
    public void shouldNotAllowTruncationBeforeCommit() throws Exception
    {
        // given
        long commitIndex = 5;
        long localTermForAllEntries = 1L;

        Outcome outcome = mock( Outcome.class );
        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        when( logMock.readEntryTerm( anyLong() ) ).thenReturn( localTermForAllEntries ); // for simplicity, all entries are at term 1
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );

        // when - then
        try
        {
            Appending.handleAppendEntriesRequest( state, outcome,
                    new RaftMessages.AppendEntries.Request( aMember, localTermForAllEntries, commitIndex - 2,
                            localTermForAllEntries,
                            new RaftLogEntry[]{
                                    new RaftLogEntry( localTermForAllEntries + 1, ReplicatedInteger.valueOf( 2 ) )},
                            commitIndex + 3 ), NullLog.getInstance() );
            fail( "Appending should not allow truncation at or before the commit index" );
        }
        catch ( IllegalStateException expected )
        {
            // fine
        }
    }

    @Test
    public void shouldNotAttemptToTruncateAtIndexBeforeTheLogPrevIndex() throws Exception
    {
        // given
        // a log with prevIndex and prevTerm set
        ReadableRaftLog logMock = mock( ReadableRaftLog.class );
        long prevIndex = 5;
        long prevTerm = 5;
        when( logMock.prevIndex() ).thenReturn( prevIndex );
        when( logMock.readEntryTerm( prevIndex ) ).thenReturn( prevTerm );
        // and which also properly returns -1 as the term for an unknown entry, in this case prevIndex - 2
        when( logMock.readEntryTerm( prevIndex - 2 ) ).thenReturn( -1L );

        // also, a state with a given commitIndex, obviously ahead of prevIndex
        long commitIndex = 10;
        ReadableRaftState state = mock( ReadableRaftState.class );
        when( state.entryLog() ).thenReturn( logMock );
        when( state.commitIndex() ).thenReturn( commitIndex );
        // which is also the append index
        when( logMock.appendIndex() ).thenReturn( commitIndex );

        // when
        // an appendEntriesRequest arrives for appending entries before the prevIndex (for whatever reason)
        Outcome outcome = mock( Outcome.class );
        Appending.handleAppendEntriesRequest( state, outcome,
                new RaftMessages.AppendEntries.Request( aMember, prevTerm, prevIndex - 2,
                        prevTerm,
                        new RaftLogEntry[]{
                                new RaftLogEntry( prevTerm, ReplicatedInteger.valueOf( 2 ) )},
                        commitIndex + 3 ), NullLog.getInstance() );

        // then
        // there should be no truncate commands. Actually, the whole thing should be a no op
        verify( outcome, times( 0 ) ).addLogCommand( any() );
    }

    private static class LogCommandMatcher extends TypeSafeMatcher<RaftLogCommand>
    {
        private final long truncateIndex;

        private LogCommandMatcher( long truncateIndex )
        {
            this.truncateIndex = truncateIndex;
        }

        @Override
        protected boolean matchesSafely( RaftLogCommand item )
        {
            return item instanceof TruncateLogCommand && ((TruncateLogCommand) item).fromIndex == truncateIndex;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( new TruncateLogCommand( truncateIndex ).toString() );
        }
    }
}
