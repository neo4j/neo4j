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

import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    private MessageHandler<RaftMessages.ClusterIdAwareMessage> raftStateMachine = mock( MessageHandler.class );
    private ClusterId localClusterId = new ClusterId( UUID.randomUUID() );

    @Test
    public void shouldInvokeInnerHandlerWhenRun() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        RaftMessages.ClusterIdAwareMessage message = new RaftMessages.ClusterIdAwareMessage(
                localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.handle( message );
        verifyZeroInteractions( raftStateMachine );

        // when
        batchHandler.run();

        // then
        verify( raftStateMachine ).handle( message );
    }

    @Test
    public void shouldInvokeHandlerOnQueuedMessage() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );
        RaftMessages.ClusterIdAwareMessage message = new RaftMessages.ClusterIdAwareMessage( localClusterId,
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
        verify( raftStateMachine ).handle( message );
    }

    @Test
    public void shouldBatchRequests() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );
        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentB = new ReplicatedString( "B" );
        RaftMessages.NewEntry.Request messageA = new RaftMessages.NewEntry.Request( null, contentA );
        RaftMessages.NewEntry.Request messageB = new RaftMessages.NewEntry.Request( null, contentB );

        batchHandler.handle( new RaftMessages.ClusterIdAwareMessage( localClusterId, messageA ) );
        batchHandler.handle( new RaftMessages.ClusterIdAwareMessage( localClusterId, messageB ) );
        verifyZeroInteractions( raftStateMachine );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentB );
        verify( raftStateMachine ).handle( new RaftMessages.ClusterIdAwareMessage( localClusterId, batchRequest ) );
    }

    @Test
    public void shouldBatchNewEntriesAndHandleOtherMessagesSingularly() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        RaftMessages.ClusterIdAwareMessage messageA = new RaftMessages.ClusterIdAwareMessage( localClusterId,
                new RaftMessages.NewEntry.Request( null, contentA ) );
        RaftMessages.ClusterIdAwareMessage messageB = new RaftMessages.ClusterIdAwareMessage( localClusterId,
                new RaftMessages.Heartbeat( null, 0, 0, 0 ) );
        RaftMessages.ClusterIdAwareMessage messageC = new RaftMessages.ClusterIdAwareMessage( localClusterId,
                new RaftMessages.NewEntry.Request( null, contentC ) );
        RaftMessages.ClusterIdAwareMessage messageD = new RaftMessages.ClusterIdAwareMessage( localClusterId,
                new RaftMessages.Heartbeat( null, 1, 1, 1 ) );

        batchHandler.handle( messageA );
        batchHandler.handle( messageB );
        batchHandler.handle( messageC );
        batchHandler.handle( messageD );
        verifyZeroInteractions( raftStateMachine );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentC );

        verify( raftStateMachine ).handle( new RaftMessages.ClusterIdAwareMessage( localClusterId, batchRequest ) );
        verify( raftStateMachine ).handle( messageB );
        verify( raftStateMachine ).handle( messageD );
    }

    @Test
    public void shouldDropMessagesAfterBeingStopped() throws Exception
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, logProvider );

        RaftMessages.ClusterIdAwareMessage message = new RaftMessages.ClusterIdAwareMessage(
                localClusterId, new RaftMessages.NewEntry.Request( null, null ) );
        batchHandler.stop();

        // when
        batchHandler.handle( message );
        batchHandler.run();

        // then
        verifyZeroInteractions( raftStateMachine );
        logProvider.assertAtLeastOnce( AssertableLogProvider.inLog( BatchingMessageHandler.class )
                .debug( "This handler has been stopped, dropping the message: %s", message ) );
    }

    @Test( timeout = 5_000 )
    public void shouldGiveUpAddingMessagesInTheQueueIfTheHandlerHasBeenStopped() throws Exception
    {
        // given
        int queueSize = 1;
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, queueSize, MAX_BATCH, NullLogProvider.getInstance() );
        RaftMessages.ClusterIdAwareMessage message = new RaftMessages.ClusterIdAwareMessage( localClusterId,
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
