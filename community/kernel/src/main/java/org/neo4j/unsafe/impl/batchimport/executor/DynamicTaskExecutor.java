/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.Exceptions.SILENT_UNCAUGHT_EXCEPTION_HANDLER;

/**
 * Implementation of {@link TaskExecutor} with a maximum queue size and where each processor is a dedicated
 * {@link Thread} pulling queued tasks and executing them.
 */
public class DynamicTaskExecutor<LOCAL> implements TaskExecutor<LOCAL>
{
    private final BlockingQueue<Task<LOCAL>> queue;
    private final ParkStrategy parkStrategy;
    private final String processorThreadNamePrefix;
    @SuppressWarnings( "unchecked" )
    private volatile Processor[] processors = (Processor[]) Array.newInstance( Processor.class, 0 );
    private volatile boolean shutDown;
    private final AtomicReference<Throwable> panic = new AtomicReference<>();
    private final Supplier<LOCAL> initialLocalState;
    private final int maxProcessorCount;

    public DynamicTaskExecutor( int initialProcessorCount, int maxProcessorCount, int maxQueueSize,
            ParkStrategy parkStrategy, String processorThreadNamePrefix )
    {
        this( initialProcessorCount, maxProcessorCount, maxQueueSize, parkStrategy, processorThreadNamePrefix,
                Suppliers.singleton( null ) );
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

        synchronized ( this )
        {
            if ( shutDown )
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
                        processors[i].processorShutDown = true;
                    }
                    this.processors = newProcessors;
                }
            }
            return processors.length;
        }
    }

    @Override
    public void submit( Task<LOCAL> task )
    {
        assertHealthy();
        try
        {
            while ( !queue.offer( task, 10, MILLISECONDS ) )
            {   // Then just stay here and try
                assertHealthy();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void assertHealthy()
    {
        Throwable panic = this.panic.get();
        if ( panic != null )
        {
            throw new TaskExecutionPanicException( "Executor has been shut down in panic", panic );
        }
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        panic.compareAndSet( null, cause );
    }

    @Override
    public synchronized void close()
    {
        if ( shutDown )
        {
            return;
        }

        while ( !queue.isEmpty() && panic.get() == null /*all bets are off in the event of panic*/ )
        {
            parkAWhile();
        }
        this.shutDown = true;
        while ( anyAlive() && panic.get() == null /*all bets are off in the event of panic*/ )
        {
            parkAWhile();
        }
    }

    @Override
    public boolean isClosed()
    {
        return shutDown;
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

    private class Processor extends Thread
    {
        // In addition to the global shutDown flag in the executor each processor has a local flag
        // so that an individual processor can be shut down, for example when reducing number of processors
        private volatile boolean processorShutDown;

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
            while ( !shutDown && !processorShutDown )
            {
                Task<LOCAL> task;
                try
                {
                    task = queue.poll( 10, MILLISECONDS );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                    break;
                }

                if ( task != null )
                {
                    try
                    {
                        task.run( threadLocalState );
                    }
                    catch ( Throwable e )
                    {
                        receivePanic( e );
                        close();
                        throw new RuntimeException( e );
                    }
                }
            }
        }
    }
}
