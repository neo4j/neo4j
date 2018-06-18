/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.Heartbeat;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import static org.neo4j.helpers.ArrayUtil.lastOf;

public class BatchingMessageHandlerTest
{
    private static final BoundedPriorityQueue.Config IN_QUEUE_CONFIG = new BoundedPriorityQueue.Config( 64, 1024 );
    private static final BatchingMessageHandler.Config BATCH_CONFIG = new BatchingMessageHandler.Config( 16, 256 );
    private final Instant now = Instant.now();
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> downstreamHandler = mock( LifecycleMessageHandler.class );
    private ClusterId localClusterId = new ClusterId( UUID.randomUUID() );
    private ContinuousJob mockJob = mock( ContinuousJob.class );
    private Function<Runnable,ContinuousJob> jobSchedulerFactory = ignored -> mockJob;

    private ExecutorService executor;
    private MemberId leader = new MemberId( UUID.randomUUID() );

    @Before
    public void before()
    {
        executor = Executors.newCachedThreadPool();
    }

    @After
    public void after() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 60, TimeUnit.SECONDS );
    }

    @Test
    public void shouldInvokeInnerHandlerWhenRun()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        NewEntry.Request message = new NewEntry.Request( null, content( "dummy" ) );

        batchHandler.handle( wrap( message ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( singletonList( new ReplicatedString( "dummy" ) ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    public void shouldInvokeHandlerOnQueuedMessage() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "dummy" );
        NewEntry.Request message = new NewEntry.Request( null, content );

        Future<?> future = executor.submit( batchHandler );

        // Some time for letting the batch handler block on its internal queue.
        //
        // It is fine if it sometimes doesn't get that far in time, just that we
        // usually want to test the wake up from blocking state.
        Thread.sleep( 50 );

        // when
        batchHandler.handle( wrap( message ) );

        // then
        future.get();
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( singletonList( content ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    public void shouldBatchRequests()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentB = new ReplicatedString( "B" );
        NewEntry.Request messageA = new NewEntry.Request( null, contentA );
        NewEntry.Request messageB = new NewEntry.Request( null, contentB );

        batchHandler.handle( wrap( messageA ) );
        batchHandler.handle( wrap( messageB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest expected = new NewEntry.BatchRequest( asList( contentA, contentB ) );
        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    public void shouldBatchUsingReceivedInstantOfFirstReceivedMessage()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "A" );
        NewEntry.Request messageA = new NewEntry.Request( null, content );

        Instant firstReceived = Instant.ofEpochMilli( 1L );
        Instant secondReceived = firstReceived.plusMillis( 1L );

        batchHandler.handle( wrap( firstReceived, messageA ) );
        batchHandler.handle( wrap( secondReceived, messageA ) );

        // when
        batchHandler.run();

        // then
        NewEntry.BatchRequest batchRequest = new NewEntry.BatchRequest( asList( content, content ) );
        verify( downstreamHandler ).handle( wrap( firstReceived, batchRequest ) );
    }

    @Test
    public void shouldBatchNewEntriesAndHandleOtherMessagesFirst()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        NewEntry.Request newEntryA = new NewEntry.Request( null, contentA );
        Heartbeat heartbeatA = new Heartbeat( null, 0, 0, 0 );
        NewEntry.Request newEntryB = new NewEntry.Request( null, contentC );
        Heartbeat heartbeatB = new Heartbeat( null, 1, 1, 1 );

        batchHandler.handle( wrap( newEntryA ) );
        batchHandler.handle( wrap( heartbeatA ) );
        batchHandler.handle( wrap( newEntryB ) );
        batchHandler.handle( wrap( heartbeatB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run(); // heartbeatA
        batchHandler.run(); // heartbeatB
        batchHandler.run(); // batchRequest

        // then
        NewEntry.BatchRequest batchRequest = new NewEntry.BatchRequest( asList( contentA, contentC ) );

        verify( downstreamHandler ).handle( wrap( heartbeatA ) );
        verify( downstreamHandler ).handle( wrap( heartbeatB ) );
        verify( downstreamHandler ).handle( wrap( batchRequest ) );
    }

    @Test
    public void shouldBatchSingleEntryAppendEntries()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry entryA = new RaftLogEntry( 0, content( "A" ) );
        RaftLogEntry entryB = new RaftLogEntry( 0, content( "B" ) );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                new RaftLogEntry[]{entryA}, leaderCommit );

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm, prevLogIndex + 1, 0,
                new RaftLogEntry[]{entryB}, leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        AppendEntries.Request expected = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                new RaftLogEntry[]{entryA, entryB}, leaderCommit );

        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    public void shouldBatchMultipleEntryAppendEntries()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry[] entriesA = entries( 0, 0, 2 );
        RaftLogEntry[] entriesB = entries( 1, 3, 3 );
        RaftLogEntry[] entriesC = entries( 2, 4, 8 );
        RaftLogEntry[] entriesD = entries( 3, 9, 15 );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesA, leaderCommit );

        prevLogIndex += appendA.entries().length;
        prevLogTerm = lastOf( appendA.entries() ).term();
        leaderCommit += 2; // arbitrary

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesB, leaderCommit );

        prevLogIndex += appendB.entries().length;
        prevLogTerm = lastOf( appendB.entries() ).term();
        leaderCommit += 5; // arbitrary

        AppendEntries.Request appendC = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                ArrayUtil.concat( entriesC, entriesD ), leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        batchHandler.handle( wrap( appendC ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        AppendEntries.Request expected = new AppendEntries.Request( leader, leaderTerm, -1, -1,
                ArrayUtil.concatArrays( entriesA, entriesB, entriesC, entriesD ), leaderCommit );

        verify( downstreamHandler ).handle( wrap( expected ) );
    }

    @Test
    public void shouldNotBatchAppendEntriesDifferentLeaderTerms()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        long leaderTerm = 1;
        long prevLogIndex = -1;
        long prevLogTerm = -1;
        long leaderCommit = 0;

        RaftLogEntry[] entriesA = entries( 0, 0, 2 );
        RaftLogEntry[] entriesB = entries( 1, 3, 3 );

        AppendEntries.Request appendA = new AppendEntries.Request( leader, leaderTerm, prevLogIndex, prevLogTerm,
                entriesA, leaderCommit );

        prevLogIndex += appendA.entries().length;
        prevLogTerm = lastOf( appendA.entries() ).term();

        AppendEntries.Request appendB = new AppendEntries.Request( leader, leaderTerm + 1, prevLogIndex, prevLogTerm,
                entriesB, leaderCommit );

        batchHandler.handle( wrap( appendA ) );
        batchHandler.handle( wrap( appendB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();
        batchHandler.run();

        // then
        verify( downstreamHandler ).handle( wrap( appendA ) );
        verify( downstreamHandler ).handle( wrap( appendB ) );
    }

    @Test
    public void shouldPrioritiseCorrectly()
    {
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        NewEntry.Request newEntry = new NewEntry.Request( null, content( "" ) );
        AppendEntries.Request append = new AppendEntries.Request( leader, 1, -1, -1,
                entries( 0, 0, 0 ), 0 );
        AppendEntries.Request emptyAppend = new AppendEntries.Request( leader, 1, -1, -1, RaftLogEntry.empty, 0 );
        Heartbeat heartbeat = new Heartbeat( null, 0, 0, 0 );

        batchHandler.handle( wrap( newEntry ) );
        batchHandler.handle( wrap( append ) );
        batchHandler.handle( wrap( heartbeat ) );
        batchHandler.handle( wrap( emptyAppend ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();
        batchHandler.run();
        batchHandler.run();
        batchHandler.run();

        // then
        InOrder inOrder = Mockito.inOrder( downstreamHandler );
        inOrder.verify( downstreamHandler ).handle( wrap( heartbeat ) );
        inOrder.verify( downstreamHandler ).handle( wrap( emptyAppend ) );
        inOrder.verify( downstreamHandler ).handle( wrap( append ) );
        inOrder.verify( downstreamHandler ).handle(
                wrap( new NewEntry.BatchRequest( singletonList( content( "" ) ) ) ) );
    }

    @Test
    public void shouldDropMessagesAfterBeingStopped() throws Throwable
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, logProvider );

        NewEntry.Request message = new NewEntry.Request( null, null );
        batchHandler.stop();

        // when
        batchHandler.handle( wrap( message ) );
        batchHandler.run();

        // then
        verify( downstreamHandler, never() ).handle(
                ArgumentMatchers.any( ReceivedInstantClusterIdAwareMessage.class ) );
        logProvider.assertAtLeastOnce( AssertableLogProvider.inLog( BatchingMessageHandler.class )
                .debug( "This handler has been stopped, dropping the message: %s", wrap( message ) ) );
    }

    @Test( timeout = 5_000 /* 5 seconds */ )
    public void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler,
                new BoundedPriorityQueue.Config( 1, 1, 1024 ), BATCH_CONFIG, jobSchedulerFactory,
                NullLogProvider.getInstance() );
        NewEntry.Request message = new NewEntry.Request( null, new ReplicatedString( "dummy" ) );
        batchHandler.handle( wrap( message ) ); // fill the queue

        CountDownLatch latch = new CountDownLatch( 1 );

        // when
        Thread thread = new Thread( () -> {
            latch.countDown();
            batchHandler.handle( wrap( message ) );
        } );

        thread.start();

        latch.await();

        batchHandler.stop();

        thread.join();

        // then we are not stuck and we terminate
    }

    @Test
    public void shouldDelegateStart() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        // when
        batchHandler.start( clusterId );

        // then
        Mockito.verify( downstreamHandler ).start( clusterId );
    }

    @Test
    public void shouldDelegateStop() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( downstreamHandler ).stop();
    }

    @Test
    public void shouldStartJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        // when
        batchHandler.start( clusterId );

        // then
        Mockito.verify( mockJob ).start();
    }

    @Test
    public void shouldStopJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, IN_QUEUE_CONFIG,
                BATCH_CONFIG, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( mockJob ).stop();
    }

    private ReceivedInstantClusterIdAwareMessage wrap( RaftMessage message )
    {
        return wrap( now, message );
    }

    private ReceivedInstantClusterIdAwareMessage<?> wrap( Instant instant, RaftMessage message )
    {
        return ReceivedInstantClusterIdAwareMessage.of( instant, localClusterId, message );
    }

    private ReplicatedContent content( String content )
    {
        return new ReplicatedString( content );
    }

    private RaftLogEntry[] entries( long term, int min, int max )
    {
        RaftLogEntry[] entries = new RaftLogEntry[max - min + 1];
        for ( int i = min; i <= max; i++ )
        {
            entries[i - min] = new RaftLogEntry( term, new ReplicatedString( String.valueOf( i ) ) );
        }
        return entries;
    }
}
