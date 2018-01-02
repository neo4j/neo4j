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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;

import static java.lang.Math.min;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Implementation of {@link TaskExecutor} with a maximum queue size and where each processor is a dedicated
 * {@link Thread} pulling queued tasks and executing them.
 */
public class DynamicTaskExecutor<LOCAL> implements TaskExecutor<LOCAL>
{
    public static final ParkStrategy DEFAULT_PARK_STRATEGY = new ParkStrategy.Park( 10 );

    private final BlockingQueue<Task<LOCAL>> queue;
    private final ParkStrategy parkStrategy;
    private final String processorThreadNamePrefix;
    @SuppressWarnings( "unchecked" )
    private volatile Processor[] processors = (Processor[]) Array.newInstance( Processor.class, 0 );
    private volatile boolean shutDown;
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
        setNumberOfProcessors( initialProcessorCount );
    }

    @Override
    public synchronized void setNumberOfProcessors( int count )
    {
        assertHealthy();
        assert count > 0;
        if ( count == processors.length )
        {
            return;
        }

        count = min( count, maxProcessorCount );
        Processor[] newProcessors;
        if ( count > processors.length )
        {   // Add one or more
            newProcessors = Arrays.copyOf( processors, count );
            for ( int i = processors.length; i < newProcessors.length; i++ )
            {
                newProcessors[i] = new Processor( processorThreadNamePrefix + "-" + i );
            }
        }
        else
        {   // Remove one or more
            newProcessors = Arrays.copyOf( processors, count );
            for ( int i = newProcessors.length; i < processors.length; i++ )
            {
                processors[i].shutDown = true;
            }
        }
        this.processors = newProcessors;
    }

    @Override
    public int numberOfProcessors()
    {
        return processors.length;
    }

    @Override
    public synchronized boolean incrementNumberOfProcessors()
    {
        if ( numberOfProcessors() >= maxProcessorCount )
        {
            return false;
        }
        setNumberOfProcessors( numberOfProcessors() + 1 );
        return true;
    }

    @Override
    public synchronized boolean decrementNumberOfProcessors()
    {
        if ( numberOfProcessors() == 1 )
        {
            return false;
        }
        setNumberOfProcessors( numberOfProcessors() - 1 );
        return true;
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
            String message = "Executor has been shut down";
            throw panic != null
                    ? new IllegalStateException( message, panic )
                    : new IllegalStateException( message );
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
    public synchronized void shutdown( boolean awaitAllCompleted )
    {
        if ( shutDown )
        {
            return;
        }

        this.shutDown = true;
        while ( awaitAllCompleted && !queue.isEmpty() && panic == null /*all bets are off in the event of panic*/ )
        {
            parkAWhile();
        }
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
        private final LOCAL threadLocalState = initialLocalState.get();

        Processor( String name )
        {
            super( name );
            setUncaughtExceptionHandler( SILENT_UNCAUGHT_EXCEPTION_HANDLER );
            start();
        }

        @Override
        public void run()
        {
            while ( !shutDown )
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
                        shutdown( false );
                        throw launderedException( e );
                    }
                }
                else
                {
                    parkAWhile();
                }
            }
        }
    }
}
