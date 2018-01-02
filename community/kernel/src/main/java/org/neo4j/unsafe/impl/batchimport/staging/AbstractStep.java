/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.LongPredicate;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.util.MovingAverage;
import org.neo4j.unsafe.impl.batchimport.stats.ProcessingStats;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;

/**
 * Basic implementation of a {@link Step}. Does the most plumbing job of building a step implementation.
 */
public abstract class AbstractStep<T> implements Step<T>
{
    private final StageControl control;
    private volatile String name;
    @SuppressWarnings( "rawtypes" )
    protected volatile Step downstream;
    private volatile boolean endOfUpstream;
    protected volatile Throwable panic;
    private volatile boolean completed;
    protected int orderingGuarantees;
    protected final LongPredicate rightDoneTicket = new LongPredicate()
    {
        @Override
        public boolean test( long ticket )
        {
            return doneBatches.get() == ticket;
        }
    };

    // Milliseconds awaiting downstream to process batches so that its queue size goes beyond the configured threshold
    // If this is big then it means that this step is faster than downstream.
    protected final AtomicLong downstreamIdleTime = new AtomicLong();
    // Milliseconds awaiting upstream to hand over batches to this step.
    // If this is big then it means that this step is faster than upstream.
    protected final AtomicLong upstreamIdleTime = new AtomicLong();
    // Number of batches received, but not yet processed.
    protected final AtomicInteger queuedBatches = new AtomicInteger();
    // Number of batches fully processed
    protected final AtomicLong doneBatches = new AtomicLong();
    // Milliseconds spent processing all received batches.
    protected final MovingAverage totalProcessingTime;
    protected long startTime, endTime;
    private final List<StatsProvider> additionalStatsProvider;

    public AbstractStep( StageControl control, String name, Configuration config,
            StatsProvider... additionalStatsProvider )
    {
        this.control = control;
        this.name = name;
        this.totalProcessingTime = new MovingAverage( config.movingAverageSize() );
        this.additionalStatsProvider = asList( additionalStatsProvider );
    }

    @Override
    public void start( int orderingGuarantees )
    {
        this.orderingGuarantees = orderingGuarantees;
        resetStats();
    }

    protected boolean guarantees( int orderingGuaranteeFlag )
    {
        return (orderingGuarantees & orderingGuaranteeFlag) != 0;
    }

    /**
     * The number of processors processing incoming batches in parallel for this step. Exposed as a method
     * since this number can change over time depending on the load.
     */
    @Override
    public int numberOfProcessors()
    {
        return 1;
    }

    @Override
    public boolean incrementNumberOfProcessors()
    {
        return false;
    }

    @Override
    public boolean decrementNumberOfProcessors()
    {
        return false;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        this.panic = cause;
    }

    protected boolean stillWorking()
    {
        if ( isPanic() )
        {   // There has been a panic, so we'll just stop working
            return false;
        }

        if ( endOfUpstream && queuedBatches.get() == 0 )
        {   // Upstream has run out and we've processed everything upstream sent us
            return false;
        }

        // We're still working
        return true;
    }

    protected boolean isPanic()
    {
        return panic != null;
    }

    @Override
    public boolean isCompleted()
    {
        return completed;
    }

    protected void issuePanic( Throwable cause )
    {
        issuePanic( cause, true );
    }

    protected void issuePanic( Throwable cause, boolean rethrow )
    {
        control.panic( cause );
        if ( rethrow )
        {
            throw Exceptions.launderedException( cause );
        }
    }

    protected long await( LongPredicate predicate, long value )
    {
        if ( predicate.test( value ) )
        {
            return 0;
        }

        long startTime = currentTimeMillis();
        for ( int i = 0; i < 1_000_000 && !predicate.test( value ); i++ )
        {   // Busy loop a while
        }

        while ( !predicate.test( value ) )
        {
            // Sleeping wait
            try
            {
                Thread.sleep( 1 );
                Thread.yield();
            }
            catch ( InterruptedException e )
            {   // It's OK
            }

            assertHealthy();
        }
        return currentTimeMillis()-startTime;
    }

    protected void assertHealthy()
    {
        if ( isPanic() )
        {
            throw new RuntimeException( "Panic called, so exiting", panic );
        }
    }

    @Override
    public void setDownstream( Step<?> downstream )
    {
        this.downstream = downstream;
    }

    @Override
    public StepStats stats()
    {
        Collection<StatsProvider> providers = new ArrayList<>();
        collectStatsProviders( providers );
        return new StepStats( name, stillWorking(), providers );
    }

    protected void collectStatsProviders( Collection<StatsProvider> into )
    {
        into.add( new ProcessingStats( doneBatches.get()+queuedBatches.get(), doneBatches.get(),
                totalProcessingTime.total(), totalProcessingTime.average() / numberOfProcessors(),
                upstreamIdleTime.get(), downstreamIdleTime.get() ) );
        into.addAll( additionalStatsProvider );
    }

    @Override
    public void endOfUpstream()
    {
        endOfUpstream = true;
        checkNotifyEndDownstream();
    }

    protected void checkNotifyEndDownstream()
    {
        if ( !stillWorking() && !isCompleted() )
        {
            synchronized ( this )
            {
                // Only allow a single thread to notify that we've ended our stream as well as calling done()
                // stillWorking(), once false cannot again return true so no need to check
                if ( !isCompleted() )
                {
                    if ( downstream != null )
                    {
                        downstream.endOfUpstream();
                    }
                    done();
                    completed = true;
                }
            }
        }
    }

    /**
     * Called once, when upstream has run out of batches to send and all received batches have been
     * processed successfully.
     */
    protected void done()
    {
        endTime = currentTimeMillis();
    }

    @Override
    public void close() throws Exception
    {   // Do nothing by default
    }

    protected void changeName( String name )
    {
        this.name = name;
    }

    protected void resetStats()
    {
        downstreamIdleTime.set( 0 );
        upstreamIdleTime.set( 0 );
        queuedBatches.set( 0 );
        doneBatches.set( 0 );
        totalProcessingTime.reset();
        startTime = currentTimeMillis();
        endTime = 0;
    }

    @Override
    public String toString()
    {
        return format( "Step[%s, processors:%d, batches:%d", name, numberOfProcessors(), doneBatches.get() );
    }
}
