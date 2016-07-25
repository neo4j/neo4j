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
package org.neo4j.coreedge.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.RaftMessages.RaftMessage;
import org.neo4j.coreedge.raft.net.Inbound.MessageHandler;
import org.neo4j.coreedge.raft.outcome.ConsensusOutcome;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BatchingMessageHandler implements Runnable, MessageHandler<RaftMessages.StoreIdAwareMessage>, MismatchedStoreIdService
{
    private final Log log;
    private final RaftInstance raftInstance;
    private final BlockingQueue<RaftMessages.StoreIdAwareMessage> messageQueue;

    private final int maxBatch;
    private final List<RaftMessages.RaftMessage> batch;

    private final LocalDatabase localDatabase;
    private RaftStateMachine raftStateMachine;
    private final List<MismatchedStoreListener> listeners = new ArrayList<>(  );

    public BatchingMessageHandler( RaftInstance raftInstance, LogProvider logProvider,
                                   int queueSize, int maxBatch, LocalDatabase localDatabase,
                                   RaftStateMachine raftStateMachine )
    {
        this.raftInstance = raftInstance;
        this.localDatabase = localDatabase;
        this.raftStateMachine = raftStateMachine;
        this.log = logProvider.getLog( getClass() );
        this.maxBatch = maxBatch;

        this.batch = new ArrayList<>( maxBatch );
        this.messageQueue = new ArrayBlockingQueue<>( queueSize );
    }

    @Override
    public void handle( RaftMessages.StoreIdAwareMessage message )
    {
        try
        {
            messageQueue.put( message );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted.", e );
        }
    }

    @Override
    public void run()
    {
        RaftMessages.StoreIdAwareMessage message = null;
        try
        {
            message = messageQueue.poll( 1, SECONDS );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted.", e );
        }

        if ( message != null )
        {
            RaftMessages.RaftMessage innerMessage = message.message();
            StoreId storeId = message.storeId();

            if ( message.storeId().equals( localDatabase.storeId() ) )
            {
                if ( messageQueue.isEmpty() )
                {
                    innerHandle( message.message() );
                }
                else
                {
                    batch.clear();
                    batch.add( innerMessage );
                    drain( messageQueue, batch, maxBatch - 1 );
                    collateAndHandleBatch( batch );
                }
            }
            else
            {
                if ( localDatabase.isEmpty() )
                {
                    log.info( "StoreId mismatch but store was empty so downloading new store from %s. Expected: " +
                            "%s, Encountered: %s. ", innerMessage.from(), storeId, localDatabase.storeId() );
                    raftStateMachine.downloadSnapshot( innerMessage.from() );
                }
                else
                {
                    log.info( "Discarding message[%s] owing to mismatched storeId and non-empty store. " +
                            "Expected: %s, Encountered: %s", innerMessage,  storeId, localDatabase.storeId() );
                    listeners.forEach( l -> {
                        MismatchedStoreIdException ex = new MismatchedStoreIdException( storeId, localDatabase.storeId() );
                        l.onMismatchedStore( ex );
                    } );
                }

            }
        }
    }

    private void innerHandle( RaftMessage raftMessage )
    {
        try
        {
            ConsensusOutcome outcome = raftInstance.handle( raftMessage );
            if ( outcome.needsFreshSnapshot() )
            {
                raftStateMachine.notifyNeedFreshSnapshot();
            }
            else
            {
                raftStateMachine.notifyCommitted( outcome.getCommitIndex());
            }
        }
        catch ( Throwable e )
        {
            raftInstance.stopTimers();
            localDatabase.panic( e );
        }
    }

    private void drain( BlockingQueue<RaftMessages.StoreIdAwareMessage> messageQueue,
                        List<RaftMessage> batch, int maxElements )
    {
        List<RaftMessages.StoreIdAwareMessage> tempDraining = new ArrayList<>();
        messageQueue.drainTo( tempDraining, maxElements );

        for ( RaftMessages.StoreIdAwareMessage storeIdAwareMessage : tempDraining )
        {
            batch.add( storeIdAwareMessage.message() );
        }
    }

    public void addMismatchedStoreListener( BatchingMessageHandler.MismatchedStoreListener listener )
    {
        listeners.add(listener);
    }

    private void collateAndHandleBatch( List<RaftMessages.RaftMessage> batch )
    {
        RaftMessages.NewEntry.Batch batchRequest = null;

        for ( RaftMessages.RaftMessage message : batch )
        {
            if ( message instanceof RaftMessages.NewEntry.Request )
            {
                RaftMessages.NewEntry.Request newEntryRequest = (RaftMessages.NewEntry.Request) message;

                if ( batchRequest == null )
                {
                    batchRequest = new RaftMessages.NewEntry.Batch( batch.size() );
                }
                batchRequest.add( newEntryRequest.content() );
            }
            else
            {
                innerHandle( message );
            }
        }

        if ( batchRequest != null )
        {
            innerHandle( batchRequest );
        }
    }
}
