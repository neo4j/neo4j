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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Implementation of {@link TaskExecutor} with a maximum queue size and where each processor is a dedicated
 * {@link Thread} pulling queued tasks and executing them.
 */
public class DynamicTaskExecutor<LOCAL> implements TaskExecutor<LOCAL>
{
    public static final ParkStrategy DEFAULT_PARK_STRATEGY = new ParkStrategy.Park( 10, MILLISECONDS );

    private final BlockingQueue<Task<LOCAL>> queue;
    private final ParkStrategy parkStrategy;
    private final String processorThreadNamePrefix;
    @SuppressWarnings( "unchecked" )
    private volatile Processor[] processors = (Processor[]) Array.newInstance( Processor.class, 0 );
    private volatile boolean shutDown;
    private volatile boolean abortQueued;
    private volatile Throwable panic;
    private final Supplier<LOCAL> initialLocalState;
    private final int maxProcessorCount;

    public DynamicTaskExecutor( int initialProcessorCount, int maxProcessorCount, int maxQueueSize,
            ParkStrategy parkStrategy, String processorThreadNamePrefix )
    {
        this( initialProcessorCount, maxProcessorCount, maxQueueSize, parkStrategy, processorThreadNamePrefix,
                Suppliers.<LOCAL>singleton( null ) );
    }

    public DynamicTaskExecutor( int initialProcessorCount, int maxProcessorCount, int maxQueueSize,
            ParkStrategy parkStrategy, String processorThreadNamePrefix, Supplier<LOCAL> initialLocalState )
    {
        this.maxProcessorCount = maxProcessorCount == 0 ? Integer.MAX_VALUE : maxProcessorCount;

        assert this.maxProcessorCount >= initialProcessorCount :
                "Unexpected initial processor count " + initialProcessorCount + " for max " + maxProcessorCount;

        this.parkStrategy = parkStrategy;
        this.processorThreadNamePrefix = processorThreadNamePrefix;
        this.initialLocalState = initialLocalState;
        this.queue = new ArrayBlockingQueue<>( maxQueueSize );
        processors( initialProcessorCount );
    }

    @Override
    public int processors( int delta )
    {
        if ( shutDown || delta == 0 )
        {
            return processors.length;
        }

        int requestedNumber = processors.length + delta;
        if ( delta > 0 )
        {
            requestedNumber = min( requestedNumber, maxProcessorCount );
            if ( requestedNumber > processors.length )
            {
                Processor[] newProcessors = Arrays.copyOf( processors, requestedNumber );
                for ( int i = processors.length; i < requestedNumber; i++ )
                {
                    newProcessors[i] = new Processor( processorThreadNamePrefix + "-" + i );
                }
                this.processors = newProcessors;
            }
        }
        else
        {
            requestedNumber = max( 1, requestedNumber );
            if ( requestedNumber < processors.length )
            {
                Processor[] newProcessors = Arrays.copyOf( processors, requestedNumber );
                for ( int i = newProcessors.length; i < processors.length; i++ )
                {
                    processors[i].shutDown = true;
                }
                this.processors = newProcessors;
            }
        }
        return processors.length;
    }

    @Override
    public void submit( Task<LOCAL> task )
    {
        assertHealthy();
        while ( !queue.offer( task ) )
        {   // Then just stay here and try
            parkAWhile();
            assertHealthy();
        }
        notifyProcessors();
    }

    @Override
    public void assertHealthy()
    {
        if ( shutDown )
        {
            if ( panic != null )
            {
                throw new IllegalStateException( "Executor has been shut down in panic", panic );
            }
            if ( abortQueued )
            {
                throw new IllegalStateException( "Executor has been shut down, aborting queued" );
            }
        }
    }

    private void notifyProcessors()
    {
        for ( Processor processor : processors )
        {
            parkStrategy.unpark( processor );
        }
    }

    @Override
    public synchronized void shutdown( int flags )
    {
        if ( shutDown )
        {
            return;
        }

        this.shutDown = true;
        boolean awaitAllCompleted = (flags & TaskExecutor.SF_AWAIT_ALL_COMPLETED) != 0;
        while ( awaitAllCompleted && !queue.isEmpty() && panic == null /*all bets are off in the event of panic*/ )
        {
            parkAWhile();
        }
        this.abortQueued = (flags & TaskExecutor.SF_ABORT_QUEUED) != 0;
        for ( Processor processor : processors )
        {
            processor.shutDown = true;
        }
        while ( awaitAllCompleted && anyAlive() && panic == null /*all bets are off in the event of panic*/ )
        {
            parkAWhile();
        }
    }

    private boolean anyAlive()
    {
        for ( Processor processor : processors )
        {
            if ( processor.isAlive() )
            {
                return true;
            }
        }
        return false;
    }

    private void parkAWhile()
    {
        parkStrategy.park( Thread.currentThread() );
    }

    private static final UncaughtExceptionHandler SILENT_UNCAUGHT_EXCEPTION_HANDLER = new UncaughtExceptionHandler()
    {
        @Override
        public void uncaughtException( Thread t, Throwable e )
        {   // Don't print about it
        }
    };

    private class Processor extends Thread
    {
        private volatile boolean shutDown;

        Processor( String name )
        {
            super( name );
            setUncaughtExceptionHandler( SILENT_UNCAUGHT_EXCEPTION_HANDLER );
            start();
        }

        @Override
        public void run()
        {
            // Initialized here since it's the thread itself that needs to call it
            final LOCAL threadLocalState = initialLocalState.get();
            while ( !abortQueued && !shutDown )
            {
                Task<LOCAL> task = queue.poll();
                if ( task != null )
                {
                    try
                    {
                        task.run( threadLocalState );
                    }
                    catch ( Throwable e )
                    {
                        panic = e;
                        shutdown( TaskExecutor.SF_ABORT_QUEUED );
                        throw launderedException( e );
                    }
                }
                else
                {
                    if ( shutDown )
                    {
                        break;
                    }
                    parkAWhile();
                }
            }
        }
    }
}
