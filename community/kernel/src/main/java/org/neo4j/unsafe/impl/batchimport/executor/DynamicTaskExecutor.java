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
package org.neo4j.unsafe.impl.batchimport.executor;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.neo4j.kernel.impl.transaction.log.ParkStrategy;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Implementation of {@link TaskExecutor} with a maximum queue size and where each processor is a dedicated
 * {@link Thread} pulling queued tasks and executing them.
 */
public class DynamicTaskExecutor implements TaskExecutor
{
    public static final ParkStrategy DEFAULT_PARK_STRATEGY = new ParkStrategy.Park( 10 );

    private final BlockingQueue<Callable<?>> queue;
    private final ParkStrategy parkStrategy;
    private final String processorThreadNamePrefix;
    private volatile Processor[] processors = new Processor[0];
    private volatile boolean shutDown;
    private volatile Throwable shutDownCause;

    public DynamicTaskExecutor( int initialProcessorCount, int maxQueueSize, ParkStrategy parkStrategy,
            String processorThreadNamePrefix )
    {
        this.parkStrategy = parkStrategy;
        this.processorThreadNamePrefix = processorThreadNamePrefix;
        this.queue = new ArrayBlockingQueue<>( maxQueueSize );
        setNumberOfProcessors( initialProcessorCount );
    }

    @Override
    public synchronized void setNumberOfProcessors( int count )
    {
        assertNotShutDown();
        assert count > 0;
        if ( count == processors.length )
        {
            return;
        }

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
    public void submit( Callable<?> task )
    {
        assertNotShutDown();
        while ( !queue.offer( task ) )
        {   // Then just stay here and try
            parkAWhile();
            assertNotShutDown();
        }
        notifyProcessors();
    }

    private void assertNotShutDown()
    {
        if ( shutDown )
        {
            String message = "Executor has been shut down";
            throw shutDownCause != null
                    ? new IllegalStateException( message, shutDownCause )
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
    public void shutdown( boolean awaitAllCompleted )
    {
        shutdown0( awaitAllCompleted, null );
    }

    private synchronized void shutdown0( boolean awaitAllCompleted, Throwable cause )
    {
        if ( shutDown )
        {
            return;
        }

        this.shutDownCause = cause;
        this.shutDown = true;
        while ( awaitAllCompleted && !queue.isEmpty() )
        {
            parkAWhile();
        }
        for ( Processor processor : processors )
        {
            processor.shutDown = true;
        }
        while ( awaitAllCompleted && anyAlive() )
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
            while ( !shutDown )
            {
                Callable<?> task = queue.poll();
                if ( task != null )
                {
                    try
                    {
                        task.call();
                    }
                    catch ( Throwable e )
                    {
                        // TODO too defensive to shut down?
                        shutdown0( false, e );
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
