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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.coreedge.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.coreedge.messaging.Inbound.MessageHandler;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BatchingMessageHandler implements Runnable, MessageHandler<RaftMessages.StoreIdAwareMessage>
{
    private final Log log;
    private final BlockingQueue<RaftMessages.StoreIdAwareMessage> messageQueue;

    private final int maxBatch;
    private final List<RaftMessages.StoreIdAwareMessage> batch;

    private MessageHandler<RaftMessages.StoreIdAwareMessage> handler;

    public BatchingMessageHandler( MessageHandler<RaftMessages.StoreIdAwareMessage> handler,
                                   int queueSize, int maxBatch, LogProvider logProvider )
    {
        this.handler = handler;
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
            if ( messageQueue.isEmpty() )
            {
                handler.handle( message );
            }
            else
            {
                batch.clear();
                batch.add( message );
                drain( messageQueue, batch, maxBatch - 1 );
                collateAndHandleBatch( batch );
            }
        }
    }

    private void drain( BlockingQueue<RaftMessages.StoreIdAwareMessage> messageQueue,
                        List<RaftMessages.StoreIdAwareMessage> batch, int maxElements )
    {
        List<RaftMessages.StoreIdAwareMessage> tempDraining = new ArrayList<>();
        messageQueue.drainTo( tempDraining, maxElements );

        for ( RaftMessages.StoreIdAwareMessage storeIdAwareMessage : tempDraining )
        {
            batch.add( storeIdAwareMessage );
        }
    }

    private void collateAndHandleBatch( List<RaftMessages.StoreIdAwareMessage> batch )
    {
        RaftMessages.NewEntry.BatchRequest batchRequest = null;
        StoreId storeId = batch.get( 0 ).storeId();

        for ( RaftMessages.StoreIdAwareMessage storeIdAwareMessage : batch )
        {
            if ( batchRequest != null && !storeIdAwareMessage.storeId().equals( storeId ))
            {
                handler.handle( new RaftMessages.StoreIdAwareMessage( storeId, batchRequest ) );
                batchRequest = null;
            }
            storeId = storeIdAwareMessage.storeId();
            RaftMessage message = storeIdAwareMessage.message();
            if ( message instanceof RaftMessages.NewEntry.Request )
            {
                RaftMessages.NewEntry.Request newEntryRequest = (RaftMessages.NewEntry.Request) message;

                if ( batchRequest == null )
                {
                    batchRequest = new RaftMessages.NewEntry.BatchRequest( batch.size() );
                }
                batchRequest.add( newEntryRequest.content() );
            }
            else
            {
                handler.handle( storeIdAwareMessage );
            }
        }

        if ( batchRequest != null )
        {
            handler.handle( new RaftMessages.StoreIdAwareMessage( storeId, batchRequest ) );
        }
    }
}
