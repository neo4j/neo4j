/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.NamedThreadFactory;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * {@link Step} that uses {@link ExecutorService} as a queue and execution mechanism.
 * Supports an arbitrary number of threads to execute batches.
 */
public abstract class ExecutorServiceStep<T> extends AbstractStep<T>
{
    private final ExecutorService executor;
    private final int workAheadSize;

    // Stats
    private final AtomicLong lastBatchEndTime = new AtomicLong();

    protected ExecutorServiceStep( StageControl control, String name, int workAheadSize, int numberOfExecutors )
    {
        super( control, name );
        this.workAheadSize = workAheadSize;
        NamedThreadFactory threadFactory = new NamedThreadFactory( name );
        this.executor = numberOfExecutors == 1 ? newSingleThreadExecutor( threadFactory ) :
            Executors.newFixedThreadPool( numberOfExecutors, threadFactory );
    }

    @Override
    public long receive( final long ticket, final T batch )
    {
        long idleTime =
                // Don't go too far ahead
                awaitDownstreamToCatchUp( workAheadSize ) +
                // Batches come in ordered
                awaitTicket( ticket );
        executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                assertHealthy();

                long startTime = startProcessingTimer();
                try
                {
                    Object result = process( ticket, batch );
                    endProcessingTimer( startTime );

                    doneBatches.incrementAndGet();
                    sendDownstream( ticket, result );
                }
                catch ( Throwable e )
                {
                    issuePanic( e );
                }
            }
        } );
        ticketReceived( ticket );

        return idleTime;
    }

    private long startProcessingTimer()
    {
        long startTime = currentTimeMillis();
        updateUpstreamIdleTime( startTime );
        return startTime;
    }

    private void endProcessingTimer( long startTime )
    {
        long endTime = currentTimeMillis();
        totalProcessingTime.addAndGet( endTime-startTime );
        lastBatchEndTime.set( endTime );
    }

    private void updateUpstreamIdleTime( long startTime )
    {
        if ( lastBatchEndTime.get() != 0 )
        {
            upstreamIdleTime.addAndGet( startTime-lastBatchEndTime.get() );
        }
    }

    private long awaitDownstreamToCatchUp( int queueSizeThreshold )
    {
        if ( receivedBatches.get() - doneBatches.get() > queueSizeThreshold )
        {
            long startTime = currentTimeMillis();
            while ( receivedBatches.get() - doneBatches.get() > queueSizeThreshold )
            {
                waitSome();
            }
            return currentTimeMillis()-startTime;
        }
        return 0;
    }

    /**
     * @return the batch object to send downstream, {@code null} for nothing to send.
     */
    protected abstract Object process( long ticket, T batch );

    @Override
    protected void done()
    {
        executor.shutdown();
    }
}
