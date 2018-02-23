/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry.BatchRequest;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry.Request;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import static org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage.of;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class BatchingMessageHandlerTest
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
    void shouldInvokeInnerHandlerWhenRun()
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
    void shouldInvokeHandlerOnQueuedMessage() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, null ) );

        ExecutorService executor = newCachedThreadPool();
        Future<?> future = executor.submit( batchHandler );

        // Some time for letting the batch handler block on its internal queue.
        //
        // It is fine if it sometimes doesn't get that far in time, just that we
        // usually want to test the wake up from blocking state.
        sleep( 50 );

        // when
        batchHandler.handle( message );

        // then
        future.get();
        verify( downstreamHandler ).handle( message );
    }

    @Test
    void shouldBatchRequests()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentB = new ReplicatedString( "B" );
        Request messageA = new Request( null, contentA );
        Request messageB = new Request( null, contentB );

        batchHandler.handle( of( now, localClusterId, messageA ) );
        batchHandler.handle( of( now, localClusterId, messageB ) );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        BatchRequest batchRequest = new BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentB );
        verify( downstreamHandler ).handle( of( now, localClusterId, batchRequest ) );
    }

    @Test
    void shouldBatchUsingReceivedInstantOfFirstReceivedMessage()
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ReplicatedString content = new ReplicatedString( "A" );
        Request messageA = new Request( null, content );

        Instant firstReceived = ofEpochMilli( 1L );
        Instant secondReceived = firstReceived.plusMillis( 1L );

        batchHandler.handle( of( firstReceived, localClusterId, messageA ) );
        batchHandler.handle( of( secondReceived, localClusterId, messageA ) );

        // when
        batchHandler.run();

        // then
        BatchRequest batchRequest = new BatchRequest( 2 );
        batchRequest.add( content );
        batchRequest.add( content );
        verify( downstreamHandler ).handle( of( firstReceived, localClusterId, batchRequest ) );
    }

    @Test
    void shouldBatchNewEntriesAndHandleOtherMessagesSingularly()
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
        BatchRequest batchRequest = new BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentC );

        verify( downstreamHandler ).handle( of( now, localClusterId, batchRequest ) );
        verify( downstreamHandler ).handle( messageB );
        verify( downstreamHandler ).handle( messageD );
    }

    @Test
    void shouldDropMessagesAfterBeingStopped() throws Throwable
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
        verify( downstreamHandler, never() ).handle( any( ReceivedInstantClusterIdAwareMessage.class ) );
        logProvider.assertAtLeastOnce( inLog( BatchingMessageHandler.class )
                .debug( "This handler has been stopped, dropping the message: %s", message ) );
    }

    @Test
    void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped()
    {
        assertTimeout( ofMillis( 5_000 ), () -> {
            //  given
            int queueSize = 1;
            BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                    downstreamHandler, queueSize, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
            RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                    new RaftMessages.NewEntry.Request( null, null ) );
            batchHandler.handle( message ); // fill the queue

            CountDownLatch latch = new CountDownLatch( 1 );

            // when
            Thread thread = new Thread( () -> {
                latch.countDown();
                batchHandler.handle( message );
            } );

            thread.start();

            latch.await();

            batchHandler.stop();

            thread.join();

            // then we are not stuck and we terminate

        } );
    }

    @Test
    void shouldDelegateStart() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        // when
        batchHandler.start( clusterId );

        // then
        verify( downstreamHandler ).start( clusterId );
    }

    @Test
    void shouldDelegateStop() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        verify( downstreamHandler ).stop();
    }

    @Test
    void shouldStartJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        // when
        batchHandler.start( clusterId );

        // then
        verify( mockJob ).start();
    }

    @Test
    void shouldStopJob() throws Throwable
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, jobSchedulerFactory, NullLogProvider.getInstance() );

        // when
        batchHandler.stop();

        // then
        verify( mockJob ).stop();
    }
}
