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
package org.neo4j.coreedge.raft.log;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class RaftLogContractTest
{
    public abstract RaftLog createRaftLog() throws Exception;

    @Test
    public void shouldCorrectReportOnEmptyLog() throws Exception
    {
        // given
        ReadableRaftLog log = createRaftLog();

        // then
        assertThat( log.appendIndex(), is( -1L ) );
        assertThat( log.commitIndex(), is( -1L ) );
        assertThat( log.entryExists( 0 ), is( false ) );
        assertThat( log.readEntryTerm( 0 ), is( -1L ) );
        assertThat( log.readEntryTerm( -1 ), is( -1L ) );
    }

    @Test
    public void shouldResetHighTermOnTruncate() throws Exception
    {
        // given
        RaftLog log = createRaftLog();
        log.append( new RaftLogEntry( 45, ReplicatedInteger.valueOf(99) ) );
        log.append( new RaftLogEntry( 46, ReplicatedInteger.valueOf(99) ) );
        log.append( new RaftLogEntry( 47, ReplicatedInteger.valueOf(99) ) );

        // truncate the last 2
        log.truncate( 1 );

        // then
        log.append( new RaftLogEntry( 46, ReplicatedInteger.valueOf(9999) ) );

        assertThat(log.readEntryTerm( 1 ), is(46L));
        assertThat(log.appendIndex(), is(1L));
    }

    @Test
    public void shouldAppendDataAndNotCommitImmediately() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( log.commitIndex(), is( -1L ) );
        assertThat( log.entryExists( 0 ), is( true ) );
        assertThat( log.readLogEntry( 0 ), equalTo( logEntry ) );
    }

    @Test
    public void shouldNotCommitWhenNoAppendedData() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLog.Listener listener = mock( RaftLog.Listener.class );
        log.registerListener( listener );

        log.commit( 10 );

        assertThat( log.appendIndex(), is( -1L ) );
        assertThat( log.commitIndex(), is( -1L ) );
        assertThat( log.entryExists( 0 ), is( false ) );
        verify( listener, never() ).onCommitted( any( ReplicatedContent.class ), anyLong() );
    }

    @Test
    public void shouldCommitOutOfOrderAppend() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLog.Listener listener = mock( RaftLog.Listener.class );
        log.registerListener( listener );

        log.commit( 10 );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        log.commit( 10 );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( log.commitIndex(), is( 0L ) );
        assertThat( log.entryExists( 0 ), is( true ) );
        verify( listener, times( 1 ) ).onCommitted( eq( logEntry.content() ), anyLong() );
    }

    @Test
    public void shouldTruncatePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( log.entryExists( 0 ), is( true ) );
        assertThat( log.entryExists( 1 ), is( true ) );

        log.truncate( 1 );

        assertThat( log.appendIndex(), is( 0L ) );
        assertThat( log.entryExists( 0 ), is( true ) );
        assertThat( log.entryExists( 1 ), is( false ) );
    }

    @Test
    public void shouldReplacePreviouslyAppendedEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );
        RaftLogEntry logEntryC = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 3 ) );
        RaftLogEntry logEntryD = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 4 ) );
        RaftLogEntry logEntryE = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 5 ) );

        log.append( logEntryA );
        log.append( logEntryB );
        log.append( logEntryC );

        log.truncate( 1 );

        log.append( logEntryD );
        log.append( logEntryE );

        assertThat( log.appendIndex(), is( 2L ) );
        assertThat( log.readLogEntry( 0 ), equalTo( logEntryA ) );
        assertThat( log.readLogEntry( 1 ), equalTo( logEntryD ) );
        assertThat( log.readLogEntry( 2 ), equalTo( logEntryE ) );
        assertThat( log.entryExists( 0 ), is( true ) );
        assertThat( log.entryExists( 1 ), is( true ) );
        assertThat( log.entryExists( 2 ), is( true ) );
        assertThat( log.entryExists( 3 ), is( false ) );
    }

    @Test
    public void shouldHaveNoEffectWhenTruncatingNonExistingEntries() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) );

        log.append( logEntryA );
        log.append( logEntryB );

        log.truncate( 5 );

        assertThat( log.appendIndex(), is( 1L ) );
        assertThat( log.readLogEntry( 0 ), equalTo( logEntryA ) );
        assertThat( log.readLogEntry( 1 ), equalTo( logEntryB ) );
    }

    @Test
    public void shouldLogDifferentContentTypes() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLogEntry logEntryA = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        RaftLogEntry logEntryB = new RaftLogEntry( 1, ReplicatedString.valueOf( "hejzxcjkzhxcjkxz" ) );

        log.append( logEntryA );
        log.append( logEntryB );

        assertThat( log.appendIndex(), is( 1L ) );

        assertThat( log.readLogEntry( 0 ), equalTo( logEntryA ) );
        assertThat( log.readLogEntry( 1 ), equalTo( logEntryB ) );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotifyAppendedEntryListener() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLog.Listener listener = mock( RaftLog.Listener.class );
        log.registerListener( listener );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );

        verify( listener, times( 1 ) ).onAppended( eq( logEntry.content() ) );
    }

    @Test
    public void shouldNotifyTruncationListener() throws Exception
    {
        RaftLog log = createRaftLog();

        RaftLog.Listener listener = mock( RaftLog.Listener.class );
        log.registerListener( listener );

        RaftLogEntry logEntry = new RaftLogEntry( 1, ReplicatedInteger.valueOf( 1 ) );
        log.append( logEntry );
        log.truncate( 0 );

        verify( listener, times( 1 ) ).onTruncated( eq( 0L ) );
    }

    @Test
    public void shouldRejectNonMonotonicTermsForEntries() throws Exception
    {
        // given
        RaftLog log = createRaftLog();

        // when
        try
        {
            log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1 ) ) );
            log.append( new RaftLogEntry( 1, ReplicatedInteger.valueOf( 2 ) ) );
            // then
            log.append( new RaftLogEntry( 0, ReplicatedInteger.valueOf( 3 ) ) );
            fail( "Should have failed because of non-monotonic terms" );
        }
        catch ( RaftStorageException expected )
        {
            // expected
        }
    }
}
