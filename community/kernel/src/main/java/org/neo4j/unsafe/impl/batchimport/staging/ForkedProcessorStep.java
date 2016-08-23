/*
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

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
    // used by forked processors to count down when they're done, so that main processing thread
    // knows when they're all done
    private final AtomicInteger doneSignal = new AtomicInteger();
    private final int maxForkedProcessors;
    protected final List<ForkedProcessor> forkedProcessors = new ArrayList<>();
    // main processing thread communicates batch to process using this variable
    // it's not volatile, but piggy-backs on globalTicket for that
    private T currentBatch;
    // this ticket helps coordinating with the forked processors
    private long globalTicket;
    // processorCount can be changed asynchronically by calls to processors(int), although its
    // changes will only be applied between processing batches as to not interfere
    private volatile int processorCount = 1;
    // forked processors can communicate errors via this variable
    private Throwable error;
    // forked processors can ping main process thread via this variable
    private Thread ticketThread;

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
            ticketThread = Thread.currentThread(); // so that forked processors can unpark
            globalTicket++;
            // ^^^ --- everything above this line will piggy-back on the volatility from the line below
            doneSignal.set( processorCount );
            notifyProcessors();
            while ( doneSignal.get() > 0 )
            {
                LockSupport.park();
            }
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
            this.id = id;
            this.localTicket = globalTicket;
            start();
        }

        @Override
        public void run()
        {
            while ( !halted )
            {
                try
                {
                    park();
                    if ( !halted && localTicket + 1 == globalTicket )
                    {
                        // ^^^ we just accessed volatile variable 'halted' and so the rest of the non-volatile
                        // variables will not be up to date for us
                        forkedProcess( id, forkedProcessors.size(), currentBatch );
                        localTicket++;
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
                    doneSignal.decrementAndGet();
                    LockSupport.unpark( ticketThread );
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
