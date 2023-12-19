/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.function.Predicates.awaitForever;

class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>>
{
    private final LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler;
    private final Log log;
    private final int maxBatch;
    private final List<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> batch;
    private final BlockingQueue<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> messageQueue;
    private final ContinuousJob job;
    private final ContentHandler contentHandler = new ContentHandler();

    private volatile boolean stopped;

    BatchingMessageHandler( LifecycleMessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler, int queueSize, int maxBatch,
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
        return delegate -> new BatchingMessageHandler( delegate, queueSize, maxBatch, jobSchedulerFactory, logProvider );
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
    public void handle( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message )
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
        RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message = null;
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

    private void drain( BlockingQueue<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> messageQueue,
                        List<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> batch, int maxElements )
    {
        List<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> tempDraining = new ArrayList<>();
        messageQueue.drainTo( tempDraining, maxElements );
        batch.addAll( tempDraining );
    }

    private void collateAndHandleBatch( List<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> batch )
    {
        RaftMessages.ReceivedInstantClusterIdAwareMessage<RaftMessages.NewEntry.BatchRequest> batchRequest = null;

        for ( RaftMessages.ReceivedInstantClusterIdAwareMessage<?> message : batch )
        {
            if ( batchRequest != null && !message.clusterId().equals( batchRequest.clusterId() ) )
            {
                handler.handle( batchRequest );
                batchRequest = null;
            }

            ReplicatedContent replicatedContent = message.dispatch( contentHandler );
            if ( replicatedContent != null )
            {
                if ( batchRequest == null )
                {
                    batchRequest =
                            RaftMessages.ReceivedInstantClusterIdAwareMessage.of(
                                    message.receivedAt(),
                                    message.clusterId(),
                                    new RaftMessages.NewEntry.BatchRequest( batch.size() )
                            );
                }
                batchRequest.message().add( replicatedContent );
            }
            else
            {
                handler.handle( message );
            }
        }

        if ( batchRequest != null )
        {
            handler.handle( batchRequest );
        }
    }

    class ContentHandler implements RaftMessages.Handler<ReplicatedContent, RuntimeException>
    {
        @Override
        public ReplicatedContent handle( RaftMessages.NewEntry.Request request ) throws RuntimeException
        {
            return request.content();
        }

        @Override
        public ReplicatedContent handle( RaftMessages.NewEntry.BatchRequest batchRequest ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.Vote.Request request ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.Vote.Response response ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.PreVote.Request request ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.PreVote.Response response ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.AppendEntries.Request request ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.AppendEntries.Response response ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.Heartbeat heartbeat ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.LogCompactionInfo logCompactionInfo ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.HeartbeatResponse heartbeatResponse ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.Timeout.Election election ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.Timeout.Heartbeat heartbeat ) throws RuntimeException
        {
            return null;
        }

        @Override
        public ReplicatedContent handle( RaftMessages.PruneRequest pruneRequest ) throws RuntimeException
        {
            return null;
        }
    }
}
