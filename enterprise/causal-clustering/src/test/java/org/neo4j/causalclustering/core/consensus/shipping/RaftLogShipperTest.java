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
package org.neo4j.causalclustering.core.consensus.shipping;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.OutboundMessageCollector;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.matchers.Matchers;
import org.neo4j.time.Clocks;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.test.matchers.Matchers.hasMessage;

public class RaftLogShipperTest
{
    @Rule
    public LifeRule life = new LifeRule( true );
    private JobScheduler scheduler = life.add( new CentralJobScheduler() );

    private OutboundMessageCollector outbound;
    private RaftLog raftLog;
    private Clock clock;
    private TimerService timerService;
    private MemberId leader;
    private MemberId follower;
    private long leaderTerm;
    private long leaderCommit;
    private long retryTimeMillis;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private LogProvider logProvider;
    private Log log;

    private RaftLogShipper logShipper;

    private RaftLogEntry entry0 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1000 ) );
    private RaftLogEntry entry1 = new RaftLogEntry( 0, ReplicatedString.valueOf( "kedha" ) );
    private RaftLogEntry entry2 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 2000 ) );
    private RaftLogEntry entry3 = new RaftLogEntry( 0, ReplicatedString.valueOf( "chupchick" ) );

    @Before
    public void setup()
    {
        // defaults
        outbound = new OutboundMessageCollector();
        raftLog = new InMemoryRaftLog();
        clock = Clocks.systemClock();
        leader = member( 0 );
        follower = member( 1 );
        leaderTerm = 0;
        leaderCommit = 0;
        retryTimeMillis = 100000;
        logProvider = mock( LogProvider.class );
        timerService = new TimerService( scheduler, logProvider );
        log = mock( Log.class );
        when( logProvider.getLog( RaftLogShipper.class ) ).thenReturn( log );
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

    private void startLogShipper()
    {
        logShipper = new RaftLogShipper( outbound, logProvider, raftLog, clock, timerService, leader, follower, leaderTerm, leaderCommit,
                        retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, new ConsecutiveInFlightCache() );
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
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, entry0.term(), RaftLogEntry.empty,
                        leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    public void shouldSendPreviousEntryOnMismatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        startLogShipper(); // ships entry2 on start

        // when
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // then: we expect it to ship (empty) entry1 next
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
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
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
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
        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2, entry3 ) ) );
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
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        raftLog.append( entry2 );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // then
        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2 ) ) );
    }

    @Test
    public void shouldNotSendNewEntriesWhenNotMatched() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();

        // when
        outbound.clear();
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // then
        assertEquals( 0, outbound.sentTo( follower ).size() );
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
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMismatch( 1, new LeaderContext( 0, 0 ) );

        // then
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 1, entry1.term(), RaftLogEntry.empty,
                        leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
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
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        while ( !outbound.sentTo( follower ).contains( expected ) )
        {
            logShipper.onMismatch( -1, new LeaderContext( 0, 0 ) );
        }

        // catchup stage
        long matchIndex;

        do
        {
            AppendEntries.Request last = (AppendEntries.Request) Iterables.last( outbound.sentTo( follower ) );
            matchIndex = last.prevLogIndex() + last.entries().length;

            outbound.clear();
            logShipper.onMatch( matchIndex, new LeaderContext( 0, 0 ) );
        }
        while ( outbound.sentTo( follower ).size() > 0 );

        assertEquals( ENTRY_COUNT - 1, matchIndex );
    }

    @Test
    public void shouldSendMostRecentlyAvailableEntryIfPruningHappened() throws IOException
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();

        //when
        raftLog.prune( 2 );
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        //then
        RaftMessages.AppendEntries.Request expected = new RaftMessages.AppendEntries.Request( leader, leaderTerm, 2,
                entry2.term(), RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    public void shouldSendLogCompactionInfoToFollowerOnMatchIfEntryHasBeenPrunedAway() throws Exception
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();

        //when
        outbound.clear();
        raftLog.prune( 2 );

        logShipper.onMatch( 1, new LeaderContext( 0, 0 ) );

        //then
        assertTrue( outbound.hasAnyEntriesTo( follower ) );
        assertThat( outbound.sentTo( follower ),
                hasMessage( new RaftMessages.LogCompactionInfo( leader, 0, 2 ) ) );
    }

    @Test
    public void shouldPickUpAfterMissedBatch() throws Exception
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );
        // we are now in PIPELINE mode, because we matched and the entire last batch was sent out

        logShipper.onTimeout();
        // and now we should be in CATCHUP mode, awaiting a late response

        // the response to the batch never came, so on timeout we enter MISMATCH mode and send a single entry based on
        // the latest we knowingly sent (entry3)
        logShipper.onTimeout();
        outbound.clear();

        // fake a match
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2, entry3 ) ) );
    }
}
