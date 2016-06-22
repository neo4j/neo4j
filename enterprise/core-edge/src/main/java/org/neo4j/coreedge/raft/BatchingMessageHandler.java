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
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BatchingMessageHandler implements Runnable, MessageHandler<RaftMessage>
{
    private final Log log;
    private final MessageHandler<RaftMessage> innerHandler;

    private final BlockingQueue<RaftMessage> messageQueue;
    private final int maxBatch;
    private final List<RaftMessage> batch;

    public BatchingMessageHandler( MessageHandler<RaftMessage> innerHandler, LogProvider logProvider,
                                   int queueSize, int maxBatch )
    {
        this.innerHandler = innerHandler;
        this.log = logProvider.getLog( getClass() );
        this.maxBatch = maxBatch;

        this.batch = new ArrayList<>( maxBatch );
        this.messageQueue = new ArrayBlockingQueue<>( queueSize );
    }

    @Override
    public void handle( RaftMessage message )
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
        RaftMessage message = null;
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
                innerHandler.handle( message );
            }
            else
            {
                batch.clear();
                batch.add( message );
                messageQueue.drainTo( batch, maxBatch - 1 );

                collateAndHandleBatch( batch );
            }
        }
    }

    private void collateAndHandleBatch( List<RaftMessage> batch )
    {
        RaftMessages.NewEntry.Batch batchRequest = null;

        for ( RaftMessage message : batch )
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
                innerHandler.handle( message );
            }
        }

        if ( batchRequest != null )
        {
            innerHandler.handle( batchRequest );
        }
    }
}
