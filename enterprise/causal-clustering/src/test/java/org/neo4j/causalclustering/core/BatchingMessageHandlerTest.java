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
package org.neo4j.causalclustering.core;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class BatchingMessageHandlerTest
{
    private static final int MAX_BATCH = 16;
    private static final int QUEUE_SIZE = 64;
    private final Instant now = Instant.now();
    @SuppressWarnings( "unchecked" )
    private LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> downstreamHandler = mock( LifecycleMessageHandler.class );
    private ClusterId localClusterId = new ClusterId( UUID.randomUUID() );
    private ContinuousJob mockJob = mock( ContinuousJob.class );
    private Function<Runnable,ContinuousJob> jobSchedulerFactory = ignored -> mockJob;

    @Test
    public void shouldInvokeInnerHandlerWhenRun()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                now, localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.handle( message );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        verify( downstreamHandler ).handle( message );
    }

    @Test
    public void shouldInvokeHandlerOnQueuedMessage() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, null ) );

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?> future = executor.submit( batchHandler );

        // Some time for letting the batch handler block on its internal queue.
        //
        // It is fine if it sometimes doesn't get that far in time, just that we
        // usually want to test the wake up from blocking state.
        Thread.sleep( 50 );

        // when
        batchHandler.handle( message );

        // then
        future.get();
        verify( downstreamHandler ).handle( message );
    }

    @Test
    public void shouldBatchRequests()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentB = new ReplicatedString( "B" );
        RaftMessages.NewEntry.Request messageA = new RaftMessages.NewEntry.Request( null, contentA );
        RaftMessages.NewEntry.Request messageB = new RaftMessages.NewEntry.Request( null, contentB );

        batchHandler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId, messageA ) );
        batchHandler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId, messageB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentB );
        verify( downstreamHandler ).handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId, batchRequest ) );
    }

    @Test
    public void shouldBatchUsingReceivedInstantOfFirstReceivedMessage()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "A" );
        RaftMessages.NewEntry.Request messageA = new RaftMessages.NewEntry.Request( null, content );

        Instant firstReceived = Instant.ofEpochMilli( 1L );
        Instant secondReceived = firstReceived.plusMillis( 1L );

        batchHandler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( firstReceived, localClusterId, messageA ) );
        batchHandler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( secondReceived, localClusterId, messageA ) );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( content );
        batchRequest.add( content );
        verify( downstreamHandler ).handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( firstReceived, localClusterId, batchRequest ) );
    }

    @Test
    public void shouldBatchNewEntriesAndHandleOtherMessagesSingularly()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> messageA = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, contentA ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> messageB = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.Heartbeat( null, 0, 0, 0 ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> messageC = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, contentC ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> messageD = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.Heartbeat( null, 1, 1, 1 ) );

        batchHandler.handle( messageA );
        batchHandler.handle( messageB );
        batchHandler.handle( messageC );
        batchHandler.handle( messageD );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentC );

        verify( downstreamHandler ).handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId, batchRequest ) );
        verify( downstreamHandler ).handle( messageB );
        verify( downstreamHandler ).handle( messageD );
    }

    @Test
    public void shouldDropMessagesAfterBeingStopped() throws Throwable
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, logProvider );

        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now,
                localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.stop();

        // when
        batchHandler.handle( message );
        batchHandler.run();

        // then
        verify( downstreamHandler, never() ).handle( ArgumentMatchers.any( RaftMessages.ReceivedInstantClusterIdAwareMessage.class ) );
        logProvider.assertAtLeastOnce( AssertableLogProvider.inLog( BatchingMessageHandler.class )
                .debug( "This handler has been stopped, dropping the message: %s", message ) );
    }

    @Test( timeout = 5_000 /* 5 seconds */ )
    public void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped() throws Throwable
    {
        // given
        int queueSize = 1;
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, queueSize, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.handle( message ); // fill the queue

        CountDownLatch latch = new CountDownLatch( 1 );

        // when
        Thread thread = new Thread( () ->
        {
            latch.countDown();
            batchHandler.handle( message );
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
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
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
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( downstreamHandler ).stop();
    }

    @Test
    public void shouldStartJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
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
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        Mockito.verify( mockJob ).stop();
    }
}
