/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.server;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.ReplicatedString;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.Inbound.MessageHandler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class BatchingMessageHandlerTest
{
    private static final int MAX_BATCH = 16;
    private static final int QUEUE_SIZE = 64;
    private final Instant now = Instant.now();
    private MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> downstreamHandler = mock( MessageHandler.class );
    private ClusterId localClusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldInvokeInnerHandlerWhenRun() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler( downstreamHandler, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        RaftMessages.ReceivedInstantClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                now, localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.handle( message );
        verifyZeroInteractions( downstreamHandler );

        // when
        batchHandler.run();

        // then
        verify( downstreamHandler ).handle( message );
    }

    @Test
    public void shouldInvokeHandlerOnQueuedMessage() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
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
    public void shouldBatchRequests() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );
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
    public void shouldBatchUsingReceivedInstantOfFirstReceivedMessage() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );
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
    public void shouldBatchNewEntriesAndHandleOtherMessagesSingularly() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        RaftMessages.ReceivedInstantClusterIdAwareMessage messageA = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, contentA ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage messageB = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.Heartbeat( null, 0, 0, 0 ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage messageC = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, contentC ) );
        RaftMessages.ReceivedInstantClusterIdAwareMessage messageD = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
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
    public void shouldDropMessagesAfterBeingStopped() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, QUEUE_SIZE, MAX_BATCH, logProvider );

        RaftMessages.ReceivedInstantClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now,
                localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.stop();

        // when
        batchHandler.handle( message );
        batchHandler.run();

        // then
        verifyZeroInteractions( downstreamHandler );
        logProvider.assertAtLeastOnce( AssertableLogProvider.inLog( BatchingMessageHandler.class )
                .debug( "This handler has been stopped, dropping the message: %s", message ) );
    }

    @Test( timeout = 5_000 /* 5 seconds */)
    public void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped() throws Exception
    {
        // given
        int queueSize = 1;
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                downstreamHandler, queueSize, MAX_BATCH, NullLogProvider.getInstance() );
        RaftMessages.ReceivedInstantClusterIdAwareMessage message = RaftMessages.ReceivedInstantClusterIdAwareMessage.of( now, localClusterId,
                new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.handle( message ); // fill the queue

        CountDownLatch latch = new CountDownLatch( 1 );

        // when
        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                latch.countDown();
                batchHandler.handle( message );
            }
        };

        thread.start();

        latch.await();

        batchHandler.stop();

        thread.join();

        // then we are not stuck and we terminate
    }
}
