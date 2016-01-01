/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.primitive.PrimitiveLongPredicate;
import org.neo4j.helpers.NamedThreadFactory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * {@link Step} that uses {@link ExecutorService} as a queue and execution mechanism.
 * Supports an arbitrary number of threads to execute batches.
 */
public abstract class ExecutorServiceStep<T> extends AbstractStep<T>
{
    private final ExecutorService executor;
    private final int workAheadSize;
    private final PrimitiveLongPredicate catchUp = new PrimitiveLongPredicate()
    {
        @Override
        public boolean accept( long queueSizeThreshold )
        {
            return queuedBatches.get() <= queueSizeThreshold;
        }
    };

    // Time stamp for when we processed the last queued batch received from upstream.
    // Useful for tracking how much time we spend waiting for batches from upstream.
    private final AtomicLong lastBatchEndTime = new AtomicLong();

    protected ExecutorServiceStep( StageControl control, String name, int workAheadSize, int numberOfExecutors )
    {
        super( control, name );
        this.workAheadSize = workAheadSize;
        NamedThreadFactory threadFactory = new NamedThreadFactory( name );
        this.executor = numberOfExecutors == 1 ?
                newSingleThreadExecutor( threadFactory ) :
                newFixedThreadPool( numberOfExecutors, threadFactory );
    }

    @Override
    public long receive( final long ticket, final T batch )
    {
        // Don't go too far ahead
        long idleTime = await( catchUp, workAheadSize );

        receivedBatch();

        executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                assertHealthy();

                long startTime = currentTimeMillis();
                try
                {
                    Object result = process( ticket, batch );
                    totalProcessingTime.addAndGet( currentTimeMillis()-startTime );

                    await( rightTicket, ticket );
                    sendDownstream( ticket, result );

                    long expectedTicket = doneBatches.incrementAndGet();
                    assert expectedTicket == ticket : "Unexpected ticket " + ticket + ", expected " + expectedTicket;

                    int queueSizeAfterThisJobDone = queuedBatches.decrementAndGet();
                    assert queueSizeAfterThisJobDone >= 0 : "Negative queue size " + queueSizeAfterThisJobDone;
                    if ( queueSizeAfterThisJobDone == 0 )
                    {
                        lastBatchEndTime.set( currentTimeMillis() );
                    }
                    checkNotifyEndDownstream();
                }
                catch ( Throwable e )
                {
                    issuePanic( e );
                }
            }
        } );

        return idleTime;
    }

    private void receivedBatch()
    {
        if ( queuedBatches.getAndIncrement() == 0 )
        {   // This is the first batch after we last drained the queue.
            long lastBatchEnd = lastBatchEndTime.get();
            if ( lastBatchEnd != 0 )
            {
                upstreamIdleTime.addAndGet( currentTimeMillis()-lastBatchEnd );
            }
        }
    }

    /**
     * @return the batch object to send downstream, {@code null} for nothing to send.
     */
    protected abstract Object process( long ticket, T batch );

    @Override
    protected void done()
    {
        executor.shutdown();
        super.done();
    }
}
