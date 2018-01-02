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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.String.format;
import static java.lang.System.nanoTime;

import static org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil.getFieldOffset;

/**
 * Executes batches by multiple threads. Each threads only processes its own part, e.g. based on node id,
 * of a batch such that all threads together fully processes the entire batch.
 * This is a very useful technique when the code processing a batch uses data structures that aren't,
 * or cannot trivially or efficiently be synchronized and access order, e.g. per node id, must be preserved.
 * This is different from {@link ProcessorStep} which has ability of running multiple batches in parallel,
 * each batch processed by one thread.
 */
public abstract class ForkedProcessorStep<T> extends AbstractStep<T>
{
    // ID 0 is the id of a processor which is always present, no matter how many or few processors
    // are assigned to process a batch. Therefore some tasks can be put on this processor, tasks
    // which may affect the batches as a whole.
    protected static final int MAIN = 0;
    private final long COMPLETED_PROCESSORS_OFFSET = getFieldOffset( Unit.class, "completedProcessors" );
    private final long PROCESSING_TIME_OFFSET = getFieldOffset( Unit.class, "processingTime" );

    private final Object[] forkedProcessors;
    private volatile int numberOfForkedProcessors;
    private final AtomicReference<Unit> head;
    private final AtomicReference<Unit> tail;
    private final Thread downstreamSender;
    private volatile int targetNumberOfProcessors = 1;
    private final int maxProcessors;
    private final int maxQueueLength;
    private volatile Thread receiverThread;
    private final StampedLock stripingLock;

    protected ForkedProcessorStep( StageControl control, String name, Configuration config, StatsProvider... statsProviders )
    {
        super( control, name, config, statsProviders );
        this.maxProcessors = config.maxNumberOfProcessors();
        this.forkedProcessors = new Object[this.maxProcessors];
        stripingLock = new StampedLock();

        Unit noop = new Unit( -1, null, 0 );
        head = new AtomicReference<>( noop );
        tail = new AtomicReference<>( noop );

        stripingLock.unlock( applyProcessorCount( stripingLock.readLock() ) );
        downstreamSender = new CompletedBatchesSender( name + " [CompletedBatchSender]" );
        maxQueueLength = 200 + maxProcessors;
    }

    private long applyProcessorCount( long lock )
    {
        if ( numberOfForkedProcessors != targetNumberOfProcessors )
        {
            stripingLock.unlock( lock );
            lock = stripingLock.writeLock();
            awaitAllCompleted();
            int processors = targetNumberOfProcessors;
            while ( numberOfForkedProcessors < processors )
            {
                if ( forkedProcessors[numberOfForkedProcessors] == null )
                {
                    forkedProcessors[numberOfForkedProcessors] = new ForkedProcessor( numberOfForkedProcessors, tail.get() );
                }
                numberOfForkedProcessors++;
            }
            if ( numberOfForkedProcessors > processors )
            {
                numberOfForkedProcessors = processors;
                // Excess processors will notice that they are not needed right now, and will park until they are.
                // The most important thing here is that future Units will have a lower number of processor as expected max.
            }
        }
        return lock;
    }

    private void awaitAllCompleted()
    {
        while ( head.get() != tail.get() )
        {
            PARK.park( receiverThread = Thread.currentThread() );
        }
    }

    @Override
    public int processors( int delta )
    {
        targetNumberOfProcessors = max( 1, min( targetNumberOfProcessors + delta, maxProcessors ) );
        return targetNumberOfProcessors;
    }

    @Override
    public void start( int orderingGuarantees )
    {
        super.start( orderingGuarantees );
        downstreamSender.start();
    }

    @Override
    public long receive( long ticket, T batch )
    {
        long time = nanoTime();
        while ( queuedBatches.get() >= maxQueueLength )
        {
            PARK.park( receiverThread = Thread.currentThread() );
        }
        // It is of importance that all items in the queue at the same time agree on the number of processors. We take this lock in order to make sure that we
        // do not interfere with another thread trying to drain the queue in order to change the processor count.
        long lock = applyProcessorCount( stripingLock.readLock() );
        queuedBatches.incrementAndGet();
        Unit unit = new Unit( ticket, batch, numberOfForkedProcessors );

        // [old head] [unit]
        //               ^
        //              head
        Unit myHead = head.getAndSet( unit );

        // [old head] -next-> [unit]
        myHead.next = unit;
        stripingLock.unlock( lock );

        return nanoTime() - time;
    }

    protected abstract void forkedProcess( int id, int processors, T batch ) throws Throwable;

    @SuppressWarnings( "unchecked" )
    void sendDownstream( Unit unit )
    {
        downstreamIdleTime.add( downstream.receive( unit.ticket, unit.batch ) );
    }

    // One unit of work. Contains the batch along with ticket and meta state during processing such
    // as how many processors are done with this batch and link to next batch in the queue.
    class Unit
    {
        private final long ticket;
        private final T batch;

        // Number of processors which is expected to process this batch, this is the number of processors
        // assigned at the time of enqueueing this unit.
        private final int processors;

        // Updated when a ForkedProcessor have processed this unit.
        // Atomic since changed by UnsafeUtil#getAndAddInt/Long.
        // Volatile since read by CompletedBatchesSender.
        @SuppressWarnings( "unused" )
        private volatile int completedProcessors;
        @SuppressWarnings( "unused" )
        private volatile long processingTime;

        // Volatile since assigned by thread enqueueing this unit after changing head of the queue.
        private volatile Unit next;

        Unit( long ticket, T batch, int processors )
        {
            this.ticket = ticket;
            this.batch = batch;
            this.processors = processors;
        }

        boolean isCompleted()
        {
            return processors > 0 && processors == completedProcessors;
        }

        void processorDone( long time )
        {
            UnsafeUtil.getAndAddLong( this, PROCESSING_TIME_OFFSET, time );
            int prevCompletedProcessors = UnsafeUtil.getAndAddInt( this, COMPLETED_PROCESSORS_OFFSET, 1 );
            assert prevCompletedProcessors < processors : prevCompletedProcessors + " vs " + processors + " for " + ticket;
        }

        @Override
        public String toString()
        {
            return format( "Unit[%d/%d]", completedProcessors, processors );
        }
    }

    /**
     * Checks tail of queue and sends fully completed units downstream. Since
     * {@link ForkedProcessorStep#receive(long, Object)} may park on queue bound, this thread will
     * unpark the most recent thread calling receive to close that wait gap.
     * {@link ForkedProcessor}, the last one processing a unit, will unpark this thread.
     */
    private final class CompletedBatchesSender extends Thread
    {
        CompletedBatchesSender( String name )
        {
            super( name );
        }

        @Override
        public void run()
        {
            Unit current = tail.get();
            while ( !isCompleted() )
            {
                Unit candidate = current.next;
                if ( candidate != null && candidate.isCompleted() )
                {
                    if ( downstream != null )
                    {
                        sendDownstream( candidate );
                    }
                    current = candidate;
                    tail.set( current );
                    queuedBatches.decrementAndGet();
                    doneBatches.incrementAndGet();
                    totalProcessingTime.add( candidate.processingTime );
                    checkNotifyEndDownstream();
                }
                else
                {
                    Thread receiver = ForkedProcessorStep.this.receiverThread;
                    if ( receiver != null )
                    {
                        PARK.unpark( receiver );
                    }
                    PARK.park( this );
                }
            }
        }
    }

    // Processes units, forever walking the queue looking for more units to process.
    // If there's no work to do it will park a while, otherwise it will exhaust the queue and process
    // as far as it can without park. No external thread unparks these forked processors.
    // So in scenarios where a processor isn't fully saturated there may be short periods of parking,
    // but should saturate without any park as long as there are units to process.
    class ForkedProcessor extends Thread
    {
        private final int id;
        private Unit current;

        ForkedProcessor( int id, Unit startingUnit )
        {
            super( name() + "-" + id );
            this.id = id;
            this.current = startingUnit;
            start();
        }

        @Override
        public void run()
        {
            try
            {
                while ( !isCompleted() )
                {
                    Unit candidate = current.next;
                    if ( candidate != null )
                    {
                        // There's work to do.
                        if ( id < candidate.processors )
                        {
                            // We are expected to take care of this one.
                            long time = nanoTime();
                            forkedProcess( id, candidate.processors, candidate.batch );
                            candidate.processorDone( nanoTime() - time );
                        }
                        // Skip to the next.

                        current = candidate;
                    }
                    else
                    {
                        // There's no work to be done right now, park a while. When we wake up and work have accumulated
                        // we'll plow throw them w/o park in between anyway.
                        PARK.park( this );
                    }
                }
            }
            catch ( Throwable e )
            {
                issuePanic( e, false );
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        Arrays.fill( forkedProcessors, null );
        super.close();
    }
}
