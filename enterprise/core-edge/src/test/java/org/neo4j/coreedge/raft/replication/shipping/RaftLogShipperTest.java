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
package org.neo4j.coreedge.raft.replication.shipping;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.coreedge.raft.LeaderContext;
import org.neo4j.coreedge.raft.OutboundMessageCollector;
import org.neo4j.coreedge.raft.RaftMessages.AppendEntries;
import org.neo4j.coreedge.raft.ReplicatedInteger;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.log.InMemoryRaftLog;
import org.neo4j.coreedge.raft.log.RaftLog;
import org.neo4j.coreedge.raft.log.RaftLogEntry;
import org.neo4j.coreedge.server.RaftTestMember;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RaftLogShipperTest
{
    OutboundMessageCollector outbound;
    RaftLog raftLog;
    Clock clock;
    RaftTestMember leader;
    RaftTestMember follower;
    long leaderTerm;
    long leaderCommit;
    long retryTimeMillis;
    int catchupBatchSize = 64;
    int maxAllowedShippingLag = 256;

    RaftLogShipper<RaftTestMember> logShipper;

    RaftLogEntry entry0 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1000 ) );
    RaftLogEntry entry1 = new RaftLogEntry( 0, ReplicatedString.valueOf( "kedha" ) );
    RaftLogEntry entry2 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 2000 ) );
    RaftLogEntry entry3 = new RaftLogEntry( 0, ReplicatedString.valueOf( "chupchick" ) );

    @Before
    public void setup()
    {
        // defaults
        outbound = new OutboundMessageCollector();
        raftLog = new InMemoryRaftLog();
        clock = Clock.SYSTEM_CLOCK;
        leader = new RaftTestMember( 0 );
        follower = new RaftTestMember( 1 );
        leaderTerm = 0;
        leaderCommit = 0;
        retryTimeMillis = 100000;
    }

    @After
    public void teardown()
    {
        if ( logShipper != null )
        {
            logShipper.stop();
            logShipper = null;
        }
    }

    public void startLogShipper()
    {
        logShipper = new RaftLogShipper<>( outbound, NullLogProvider.getInstance(), raftLog,
                clock, leader, follower, leaderTerm, leaderCommit, retryTimeMillis,
                catchupBatchSize, maxAllowedShippingLag );
        logShipper.start();
    }

    @Test
    public void shouldSendLastEntryOnStart() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );

        // when
        startLogShipper();

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry1 ) );
    }

    @Test
    public void shouldSendPreviousEntryOnMismatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        startLogShipper();

        // when
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry0 ) );
    }

    @Test
    public void shouldKeepSendingFirstEntryAfterSeveralMismatches() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        startLogShipper();

        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry0 ) );
    }

    @Test
    public void shouldSendNextBatchAfterMatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );
        startLogShipper();

        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry1, entry2, entry3 ) );
    }

    @Test
    public void shouldSendNewEntriesAfterMatchingLastEntry() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();

        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();

        raftLog.append( entry1 );
        logShipper.onNewEntry( 0, 0, entry1, new LeaderContext( 0, 0 ) );
        raftLog.append( entry2 );
        logShipper.onNewEntry( 1, 0, entry2, new LeaderContext( 0, 0 ) );

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry1, entry2 ) );
    }

    @Test
    public void shouldNotSendNewEntriesWhenNotMatched() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();

        // when
        outbound.clear();
        logShipper.onNewEntry( 0, 0, entry1, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntry( 1, 0, entry2, new LeaderContext( 0, 0 ) );

        // then
        assertEquals( outbound.sentTo( follower ).size(), 0 );
    }

    @Test
    public void shouldResendLastSentEntryOnFirstMismatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();
        raftLog.append( entry1 );
        raftLog.append( entry2 );

        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntry( 0, 0, entry1, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntry( 1, 0, entry2, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMismatch( 1, new LeaderContext( 0, 0 ) );

        // then
        assertTrue( outbound.hasEntriesTo( follower, entry2 ) );
    }

    @Test
    public void shouldSendAllEntriesAndCatchupCompletely() throws Throwable
    {
        // given
        final int ENTRY_COUNT = catchupBatchSize * 10;
        Collection<RaftLogEntry> entries = new ArrayList<>();
        for ( int i = 0; i < ENTRY_COUNT; i++ )
        {
            entries.add( new RaftLogEntry( 0, ReplicatedInteger.valueOf( i ) ) );
        }

        for ( RaftLogEntry entry : entries )
        {
            raftLog.append( entry );
        }

        // then
        startLogShipper();

        // back-tracking stage
        RaftLogEntry firstEntry = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 0 ) );
        while ( !outbound.hasEntriesTo( follower, firstEntry ) )
        {
            logShipper.onMismatch( -1, new LeaderContext( 0, 0 ) );
        }

        // catchup stage
        long matchIndex;

        do
        {
            AppendEntries.Request last = (AppendEntries.Request) IteratorUtil.last( outbound.sentTo( follower ) );
            matchIndex = last.prevLogIndex() + last.entries().length;

            outbound.clear();
            logShipper.onMatch( matchIndex, new LeaderContext( 0, 0 ) );
        }
        while ( outbound.sentTo( follower ).size() > 0 );

        assertEquals( ENTRY_COUNT-1, matchIndex );
    }
}
