/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.unsafe.impl.batchimport.Configuration;

import static java.util.concurrent.locks.LockSupport.park;

/**
 * Executes batches sequentially, one at a time, although running multiple processing threads on each batch.
 * This is almost the opposite of {@link ProcessorStep} which has ability of running multiple batches in parallel,
 * each batch processed by one thread.
 *
 * The purpose of this type of step is to much better be able to parallelize steps that are seen to be
 * bottlenecks, but are generally very hard to figure out how to parallelize.
 *
 * Extending {@link ProcessorStep} and providing max processors 1, i.e. always single-threaded, i.e. only
 * max one batch being processed at any given point in time. This thread instead starts its own army of
 * internal "forked" processors which will sit and wait for notifications to start processing the next batch.
 */
public abstract class ForkedProcessorStep<T> extends ProcessorStep<T>
{
    // ID 0 is the id of a processor which is always present, no matter how many or few processors
    // are assigned to process a batch. Therefore some tasks can be put on this processor, tasks
    // which may affect the batches as a whole.
    protected static final int MAIN = 0;

    // used by forked processors to count down when they're done, so that main processing thread
    // knows when they're all done
    private final AtomicInteger doneSignal = new AtomicInteger();
    private final int maxForkedProcessors;
    protected final List<ForkedProcessor> forkedProcessors = new ArrayList<>();
    // main processing thread communicates batch to process using this variable
    // it's not volatile, but piggy-backs on globalTicket for that
    private T currentBatch;
    // this ticket helps coordinating with the forked processors. It's checked by the forked processors
    // and so acts as a useful memory barrier for other variables
    private volatile long globalTicket;
    // processorCount can be changed asynchronically by calls to processors(int), although its
    // changes will only be applied between processing batches as to not interfere
    private volatile int processorCount = 1;
    // forked processors can communicate errors via this variable.
    // Doesn't need to be volatile - piggy-backs off of doneSignal access between submitter thread
    // and the failing processor thread
    private Throwable error;
    // represents the submitter thread which called process() method. Forked processor threads can
    // ping/unpark submitter thread via this variable. Piggy-backs off of globalTicket/doneSignal.
    private Thread submitterThread;

    protected ForkedProcessorStep( StageControl control, String name, Configuration config, int maxProcessors )
    {
        super( control, name, config, 1 );
        this.maxForkedProcessors = maxProcessors == 0 ? config.maxNumberOfProcessors() : maxProcessors;
        applyProcessorCount();
    }

    @Override
    protected void process( T batch, BatchSender sender ) throws Throwable
    {
        applyProcessorCount();
        int processorCount = forkedProcessors.size();
        if ( processorCount == 1 )
        {
            // No need to complicate things, just do the "forked" processing right here
            forkedProcess( 0, 1, batch );
        }
        else
        {
            // Multiple processors, hand over the state to the processors and let them loose
            currentBatch = batch;
            submitterThread = Thread.currentThread(); // so that forked processors can unpark
            // ^^^ --- everything above this line will piggy-back on the volatility from globalTicket
            doneSignal.set( processorCount );
            globalTicket++;
            notifyProcessors();
            while ( doneSignal.get() > 0 )
            {
                LockSupport.park();
            }
            // any write to "error" is now visible to us because of our check (and forked processor's write)
            // to doneSignal
            if ( error != null )
            {
                throw error;
            }
        }

        if ( downstream != null )
        {
            sender.send( batch );
        }
    }

    private void notifyProcessors()
    {
        for ( int i = 0; i < forkedProcessors.size(); i++ )
        {
            LockSupport.unpark( forkedProcessors.get( i ) );
        }
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        for ( ForkedProcessor forkedProcessor : forkedProcessors )
        {
            forkedProcessor.halt();
        }
    }

    /**
     * This method is called by one of the threads processing this batch, there are multiple threads processing
     * this batch in parallel, each with its own {@code id}.
     *
     * @param id zero-based id of this thread
     * @param processors number of processors concurrently processing this batch
     * @param batch batch to process
     */
    protected abstract void forkedProcess( int id, int processors, T batch );

    private void applyProcessorCount()
    {
        int processorCount = this.processorCount;
        while ( processorCount != forkedProcessors.size() )
        {
            if ( forkedProcessors.size() < processorCount )
            {
                forkedProcessors.add( new ForkedProcessor( forkedProcessors.size() ) );
            }
            else
            {
                forkedProcessors.remove( forkedProcessors.size() - 1 ).halt();
            }
        }
    }

    @Override
    public int processors( int delta )
    {
        // Don't delegate to ProcessorStep, because we're not parallelizing on batches, we're parallelizing
        // inside each batch and batches must be processed in order
        int processors = this.processorCount;
        processors += delta;
        if ( processors < 1 )
        {
            processors = 1;
        }
        if ( processors > maxForkedProcessors )
        {
            processors = maxForkedProcessors;
        }
        return this.processorCount = processors;
    }

    class ForkedProcessor extends Thread
    {
        private final int id;
        private volatile boolean halted;
        private long localTicket;

        ForkedProcessor( int id )
        {
            super( name() + "-" + id );
            this.id = id;
            this.localTicket = globalTicket;
            start();
        }

        @Override
        public void run()
        {
            while ( !halted )
            {
                boolean processed = false;
                try
                {
                    park();
                    if ( !halted && localTicket + 1 == globalTicket )
                    {
                        // ^^^ we just accessed volatile variable 'globalTicket' and so currentBatch and
                        // forkedProcessors will now be up to date for us
                        processed = true;
                        forkedProcess( id, forkedProcessors.size(), currentBatch );
                    }
                }
                catch ( Throwable t )
                {
                    error = t;
                }
                finally
                {
                    // ^^^ finish off with counting down doneSignal which serves two purposes:
                    // - notifying the main submitter thread that we're done
                    // - going through a volatile memory access to let our changes propagate
                    if ( processed )
                    {
                        localTicket++;
                        doneSignal.decrementAndGet();
                        LockSupport.unpark( submitterThread );
                    }
                }
            }
        }

        void halt()
        {
            halted = true;
            LockSupport.unpark( this );
        }
    }
}
