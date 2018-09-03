/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.BoundedPriorityQueue.Removable;
import org.neo4j.causalclustering.core.consensus.ContinuousJob;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import org.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry;
import org.neo4j.causalclustering.core.consensus.RaftMessages.ReceivedInstantClusterIdAwareMessage;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.messaging.ComposableMessageHandler;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.stream.Streams;

import static java.lang.Long.max;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.OK;
import static org.neo4j.helpers.ArrayUtil.lastOf;

/**
 * This class gets Raft messages as input and queues them up for processing. Some messages are
 * batched together before they are forwarded to the Raft machine, for reasons of efficiency.
 */
class BatchingMessageHandler implements Runnable, LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>>
{
    public static class Config
    {
        private final int maxBatchCount;
        private final long maxBatchBytes;

        Config( int maxBatchCount, long maxBatchBytes )
        {
            this.maxBatchCount = maxBatchCount;
            this.maxBatchBytes = maxBatchBytes;
        }
    }

    private final LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> handler;
    private final Log log;
    private final BoundedPriorityQueue<ReceivedInstantClusterIdAwareMessage<?>> inQueue;
    private final ContinuousJob job;
    private final List<ReplicatedContent> contentBatch; // reused for efficiency
    private final List<RaftLogEntry> entryBatch; // reused for efficiency
    private final Config batchConfig;

    private volatile boolean stopped;
    private volatile BoundedPriorityQueue.Result lastResult = OK;
    private AtomicLong droppedCount = new AtomicLong();

    BatchingMessageHandler( LifecycleMessageHandler<ReceivedInstantClusterIdAwareMessage<?>> handler,
            BoundedPriorityQueue.Config inQueueConfig, Config batchConfig, Function<Runnable,ContinuousJob> jobFactory,
            LogProvider logProvider )
    {
        this.handler = handler;
        this.log = logProvider.getLog( getClass() );
        this.batchConfig = batchConfig;
        this.contentBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.entryBatch = new ArrayList<>( batchConfig.maxBatchCount );
        this.inQueue = new BoundedPriorityQueue<>( inQueueConfig, ContentSize::of, new MessagePriority() );
        this.job = jobFactory.apply( this );
    }

    static ComposableMessageHandler composable( BoundedPriorityQueue.Config inQueueConfig, Config batchConfig,
            Function<Runnable,ContinuousJob> jobSchedulerFactory, LogProvider logProvider )
    {
        return delegate -> new BatchingMessageHandler( delegate, inQueueConfig, batchConfig, jobSchedulerFactory,
                logProvider );
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
    public void handle( ReceivedInstantClusterIdAwareMessage<?> message )
    {
        if ( stopped )
        {
            log.debug( "This handler has been stopped, dropping the message: %s", message );
            return;
        }

        BoundedPriorityQueue.Result result = inQueue.offer( message );
        logQueueState( result );
    }

    private void logQueueState( BoundedPriorityQueue.Result result )
    {
        if ( result != OK )
        {
            droppedCount.incrementAndGet();
        }

        if ( result != lastResult )
        {
            if ( result == OK )
            {
                log.info( "Raft in-queue not dropping messages anymore. Dropped %d messages.",
                        droppedCount.getAndSet( 0 ) );
            }
            else
            {
                log.warn( "Raft in-queue dropping messages after: " + result );
            }
            lastResult = result;
        }
    }

    @Override
    public void run()
    {
        Optional<ReceivedInstantClusterIdAwareMessage<?>> baseMessage;
        try
        {
            baseMessage = inQueue.poll( 1, SECONDS );
        }
        catch ( InterruptedException e )
        {
            log.warn( "Not expecting to be interrupted.", e );
            return;
        }

        if ( !baseMessage.isPresent() )
        {
            return;
        }

        ReceivedInstantClusterIdAwareMessage batchedMessage = baseMessage.get().message().dispatch(
                new BatchingHandler( baseMessage.get() ) );

        handler.handle( batchedMessage == null ? baseMessage.get() : batchedMessage );
    }

    /**
     * Batches together the content of NewEntry.Requests for efficient handling.
     */
    private NewEntry.BatchRequest batchNewEntries( NewEntry.Request first )
    {
        contentBatch.clear();

        contentBatch.add( first.content() );
        long totalBytes = first.content().size().orElse( 0L );

        while ( contentBatch.size() < batchConfig.maxBatchCount )
        {
            Optional<Removable<NewEntry.Request>> peeked = peekNext( NewEntry.Request.class );

            if ( !peeked.isPresent() )
            {
                break;
            }

            ReplicatedContent content = peeked.get().get().content();

            if ( content.size().isPresent() && (totalBytes + content.size().getAsLong()) > batchConfig.maxBatchBytes )
            {
                break;
            }

            contentBatch.add( content );

            boolean removed = peeked.get().remove();
            assert removed; // single consumer assumed
        }

        /*
         * Individual NewEntry.Requests are batched together into a BatchRequest to take advantage
         * of group commit into the Raft log and any other batching benefits.
         */
        return new NewEntry.BatchRequest( contentBatch );
    }

    private AppendEntries.Request batchAppendEntries( AppendEntries.Request first )
    {
        entryBatch.clear();

        long totalBytes = 0;

        for ( RaftLogEntry entry : first.entries() )
        {
            totalBytes += entry.content().size().orElse( 0L );
            entryBatch.add( entry );
        }

        long leaderCommit = first.leaderCommit();
        long lastTerm = lastOf( first.entries() ).term();

        while ( entryBatch.size() < batchConfig.maxBatchCount )
        {
            Optional<Removable<AppendEntries.Request>> peeked = peekNext( AppendEntries.Request.class );

            if ( !peeked.isPresent() )
            {
                break;
            }

            AppendEntries.Request request = peeked.get().get();

            if ( request.entries().length == 0 || !consecutiveOrigin( first, request, entryBatch.size() ) )
            {
                // probe (RaftLogShipper#sendEmpty) or leader switch
                break;
            }

            assert lastTerm == request.prevLogTerm();

            // note that this code is backwards compatible, but AppendEntries.Request generation by the leader
            // will be changed to only generate single entry AppendEntries.Requests and the code here
            // will be responsible for the batching of the individual and consecutive entries

            RaftLogEntry[] entries = request.entries();
            lastTerm = lastOf( entries ).term();

            if ( entries.length + entryBatch.size() > batchConfig.maxBatchCount )
            {
                break;
            }

            long requestBytes = Arrays.stream( entries )
                    .mapToLong( entry -> entry.content().size().orElse( 0L ) )
                    .sum();

            if ( requestBytes > 0 && (totalBytes + requestBytes) > batchConfig.maxBatchBytes )
            {
                break;
            }

            entryBatch.addAll( Arrays.asList( entries ) );
            totalBytes += requestBytes;
            leaderCommit = max( leaderCommit, request.leaderCommit() );

            boolean removed = peeked.get().remove();
            assert removed; // single consumer assumed
        }

        return new AppendEntries.Request( first.from(), first.leaderTerm(), first.prevLogIndex(), first.prevLogTerm(),
                entryBatch.toArray( RaftLogEntry.empty ), leaderCommit );
    }

    private boolean consecutiveOrigin( AppendEntries.Request first, AppendEntries.Request request, int currentSize )
    {
        if ( request.leaderTerm() != first.leaderTerm() )
        {
            return false;
        }
        else
        {
            return request.prevLogIndex() == first.prevLogIndex() + currentSize;
        }
    }

    private <M> Optional<Removable<M>> peekNext( Class<M> acceptedType )
    {
        return inQueue.peek()
                .filter( r -> acceptedType.isInstance( r.get().message() ) )
                .map( r -> r.map( m -> acceptedType.cast( m.message() ) ) );
    }

    private static class ContentSize extends RaftMessages.HandlerAdaptor<Long,RuntimeException>
    {
        private static final ContentSize INSTANCE = new ContentSize();

        private ContentSize()
        {
        }

        static long of( ReceivedInstantClusterIdAwareMessage<?> message )
        {
            Long dispatch = message.dispatch( INSTANCE );
            return dispatch == null ? 0L : dispatch;
        }

        @Override
        public Long handle( NewEntry.Request request ) throws RuntimeException
        {
            return request.content().size().orElse( 0L );
        }

        @Override
        public Long handle( AppendEntries.Request request ) throws RuntimeException
        {
            long totalSize = 0L;
            for ( RaftLogEntry entry : request.entries() )
            {
                if ( entry.content().size().isPresent() )
                {
                    totalSize += entry.content().size().getAsLong();
                }
            }
            return totalSize;
        }
    }

    private class MessagePriority extends RaftMessages.HandlerAdaptor<Integer,RuntimeException>
            implements Comparator<ReceivedInstantClusterIdAwareMessage<?>>
    {
        private final Integer BASE_PRIORITY = 10; // lower number means higher priority

        @Override
        public Integer handle( AppendEntries.Request request )
        {

            // 0 length means this is a heartbeat, so let it be handled with higher priority
            return request.entries().length == 0 ? BASE_PRIORITY : 20;
        }

        @Override
        public Integer handle( NewEntry.Request request )
        {
            return 30;
        }

        @Override
        public int compare( ReceivedInstantClusterIdAwareMessage<?> messageA,
                ReceivedInstantClusterIdAwareMessage<?> messageB )
        {
            int priorityA = getPriority( messageA );
            int priorityB = getPriority( messageB );

            return Integer.compare( priorityA, priorityB );
        }

        private int getPriority( ReceivedInstantClusterIdAwareMessage<?> message )
        {
            Integer priority = message.dispatch( this );
            return priority == null ? BASE_PRIORITY : priority;
        }
    }

    private class BatchingHandler extends RaftMessages.HandlerAdaptor<ReceivedInstantClusterIdAwareMessage,RuntimeException>
    {
        private final ReceivedInstantClusterIdAwareMessage<?> baseMessage;

        BatchingHandler( ReceivedInstantClusterIdAwareMessage<?> baseMessage )
        {
            this.baseMessage = baseMessage;
        }

        @Override
        public ReceivedInstantClusterIdAwareMessage handle( NewEntry.Request request ) throws RuntimeException
        {
            NewEntry.BatchRequest newEntryBatch = batchNewEntries( request );
            return ReceivedInstantClusterIdAwareMessage.of( baseMessage.receivedAt(), baseMessage.clusterId(), newEntryBatch );
        }

        @Override
        public ReceivedInstantClusterIdAwareMessage handle( AppendEntries.Request request ) throws
                RuntimeException
        {
            if ( request.entries().length == 0 )
            {
                // this is a heartbeat, so let it be solo handled
                return null;
            }

            AppendEntries.Request appendEntriesBatch = batchAppendEntries( request );
            return ReceivedInstantClusterIdAwareMessage.of( baseMessage.receivedAt(), baseMessage.clusterId(), appendEntriesBatch );
        }
    }
}
