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
package org.neo4j.causalclustering.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.RaftMessage;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.function.Predicates.awaitForever;

class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> handler;
    private final Log log;
    private final int maxBatch;
    private final List<RaftMessages.ReceivedInstantClusterIdAwareMessage> batch;
    private final BlockingQueue<RaftMessages.ReceivedInstantClusterIdAwareMessage> messageQueue;
    private final ContinuousJob job;

    private volatile boolean stopped;

    BatchingMessageHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> handler, int queueSize, int maxBatch,
            Function<Runnable,ContinuousJob> jobSchedulerFactory, LogProvider logProvider )
    {
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
        this.maxBatch = maxBatch;
        this.batch = new ArrayList<>( maxBatch );
        this.messageQueue = new ArrayBlockingQueue<>( queueSize );
        job = jobSchedulerFactory.apply( this );
    }

    static ComposableMessageHandler composable( int queueSize, int maxBatch, Function<Runnable,ContinuousJob> jobSchedulerFactory, LogProvider logProvider )
    {
        return (LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage> delegate) ->
                new BatchingMessageHandler( delegate, queueSize, maxBatch, jobSchedulerFactory, logProvider );
    }
    @Override
    public void start( ClusterId clusterId ) throws Throwable
    {
        handler.start( clusterId );
        job.start();
    }

    @Override
    public void stop() throws Throwable
    {
        stopped = true;
        handler.stop();
        job.stop();
    }

    @Override
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage message )
    {
        if ( stopped )
        {
            log.debug( "This handler has been stopped, dropping the message: %s", message );
            return;
        }

        // keep trying to add the message into the queue, give up only if this component has been stopped
        awaitForever( () -> stopped || messageQueue.offer( message ), 100, TimeUnit.MILLISECONDS );
    }

    @Override
    public void run()
    {
        RaftMessages.ReceivedInstantClusterIdAwareMessage message = null;
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

    private void drain( BlockingQueue<RaftMessages.ReceivedInstantClusterIdAwareMessage> messageQueue,
                        List<RaftMessages.ReceivedInstantClusterIdAwareMessage> batch, int maxElements )
    {
        List<RaftMessages.ReceivedInstantClusterIdAwareMessage> tempDraining = new ArrayList<>();
        messageQueue.drainTo( tempDraining, maxElements );
        batch.addAll( tempDraining );
    }

    // TODO consider refactoring
    // BatchRequest is cluster & received at aware
    // Replace instanceOf with visitor
    private void collateAndHandleBatch( List<RaftMessages.ReceivedInstantClusterIdAwareMessage> batch )
    {
        RaftMessages.ReceivedInstantClusterIdAwareMessage firstMessage = batch.get( 0 );
        RaftMessages.NewEntry.BatchRequest batchRequest = null;
        Instant firstReceivedAt = firstMessage.receivedAt();
        ClusterId clusterId = firstMessage.clusterId();

        for ( RaftMessages.ReceivedInstantClusterIdAwareMessage wrappedMessage : batch )
        {
            if ( batchRequest != null && !wrappedMessage.clusterId().equals( clusterId ))
            {
                handler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( firstReceivedAt, clusterId, batchRequest ) );
                batchRequest = null;
            }
            clusterId = wrappedMessage.clusterId();
            RaftMessage message = wrappedMessage.message();
            if ( message instanceof RaftMessages.NewEntry.Request )
            {
                RaftMessages.NewEntry.Request newEntryRequest = (RaftMessages.NewEntry.Request) message;

                if ( batchRequest == null )
                {
                    batchRequest = new RaftMessages.NewEntry.BatchRequest( batch.size() );
                    firstReceivedAt = wrappedMessage.receivedAt();
                }
                batchRequest.add( newEntryRequest.content() );
            }
            else
            {
                handler.handle( wrappedMessage );
            }
        }

        if ( batchRequest != null )
        {
            handler.handle( RaftMessages.ReceivedInstantClusterIdAwareMessage.of( firstReceivedAt, clusterId, batchRequest ) );
        }
    }
}
