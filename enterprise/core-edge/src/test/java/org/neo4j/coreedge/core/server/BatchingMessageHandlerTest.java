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
package org.neo4j.coreedge.core.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.ReplicatedString;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.messaging.Inbound.MessageHandler;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class BatchingMessageHandlerTest
{
    private static final int MAX_BATCH = 16;
    private static final int QUEUE_SIZE = 64;
    private LocalDatabase localDatabase = mock( LocalDatabase.class );
    private MessageHandler<RaftMessages.StoreIdAwareMessage> raftStateMachine = mock( MessageHandler.class );
    private StoreId localStoreId = new StoreId( 1, 2, 3, 4 );

    @Before
    public void setup()
    {
        when( localDatabase.storeId() ).thenReturn( localStoreId );
    }

    @Test
    public void shouldInvokeInnerHandlerWhenRun() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        RaftMessages.StoreIdAwareMessage message = new RaftMessages.StoreIdAwareMessage(
                localStoreId, new RaftMessages.NewEntry.Request( null, null ) );
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
        RaftMessages.StoreIdAwareMessage message = new RaftMessages.StoreIdAwareMessage( localStoreId,
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

        batchHandler.handle( new RaftMessages.StoreIdAwareMessage( localStoreId, messageA ) );
        batchHandler.handle( new RaftMessages.StoreIdAwareMessage( localStoreId, messageB ) );
        verifyZeroInteractions( raftStateMachine );

        // when
        batchHandler.run();

        // then
        RaftMessages.NewEntry.BatchRequest batchRequest = new RaftMessages.NewEntry.BatchRequest( 2 );
        batchRequest.add( contentA );
        batchRequest.add( contentB );
        verify( raftStateMachine ).handle( new RaftMessages.StoreIdAwareMessage( localStoreId, batchRequest ) );
    }

    @Test
    public void shouldBatchNewEntriesAndHandleOtherMessagesSingularly() throws Exception
    {
        // given
        BatchingMessageHandler batchHandler = new BatchingMessageHandler(
                raftStateMachine, QUEUE_SIZE, MAX_BATCH, NullLogProvider.getInstance() );

        ReplicatedString contentA = new ReplicatedString( "A" );
        ReplicatedString contentC = new ReplicatedString( "C" );

        RaftMessages.StoreIdAwareMessage messageA = new RaftMessages.StoreIdAwareMessage( localStoreId,
                new RaftMessages.NewEntry.Request( null, contentA ) );
        RaftMessages.StoreIdAwareMessage messageB = new RaftMessages.StoreIdAwareMessage( localStoreId,
                new RaftMessages.Heartbeat( null, 0, 0, 0 ) );
        RaftMessages.StoreIdAwareMessage messageC = new RaftMessages.StoreIdAwareMessage( localStoreId,
                new RaftMessages.NewEntry.Request( null, contentC ) );
        RaftMessages.StoreIdAwareMessage messageD = new RaftMessages.StoreIdAwareMessage( localStoreId,
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

        verify( raftStateMachine ).handle( new RaftMessages.StoreIdAwareMessage( localStoreId, batchRequest ) );
        verify( raftStateMachine ).handle( messageB );
        verify( raftStateMachine ).handle( messageD );
    }
}
