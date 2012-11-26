/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.test;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Executes {@link WorkerCommand}s in another thread. Very useful for writing
 * tests which handles two simultaneous transactions and interleave them,
 * f.ex for testing locking and data visibility.
 * 
 * @author Mattias Persson
 *
 * @param <T>
 */
public class OtherThreadExecutor<T> implements ThreadFactory
{
    private static final AtomicLong THREADID = new AtomicLong();
    private final ExecutorService commandExecutor = newSingleThreadExecutor(this);
    private final T state;
    private volatile Thread thread;
    private volatile boolean executingCommand;

    public OtherThreadExecutor( T initialState )
    {
        this.state = initialState;
    }

    public <R> Future<R> executeDontWait( final WorkerCommand<T, R> cmd ) throws Exception
    {
        return commandExecutor.submit( new Callable<R>()
        {
            @Override
            public R call()
            {
                executingCommand = true;
                try
                {
                    return cmd.doWork( state );
                }
                finally
                {
                    executingCommand = false;
                }
            }
        } );
    }
    
    public <R> R execute( WorkerCommand<T, R> cmd ) throws Exception
    {
        return executeDontWait( cmd ).get();
    }

    public <R> R execute( WorkerCommand<T, R> cmd, long timeout ) throws Exception
    {
        Future<R> future = executeDontWait( cmd );
        boolean success = false;
        try
        {
            R result = future.get( timeout, TimeUnit.MILLISECONDS );
            success = true;
            return result;
        }
        finally
        {
            if ( !success ) future.cancel( true );
        }
    }
    
    public interface WorkerCommand<T, R>
    {
        R doWork( T state );
    }

    @Override
    public Thread newThread( Runnable r )
    {
        Thread thread = new Thread( r, getClass().getName() + ":" + THREADID.getAndIncrement() )
        {
            @Override
            public void run()
            {
                try
                {
                    super.run();
                }
                finally
                {
                    OtherThreadExecutor.this.thread = null;
                }
            }
        };
        this.thread = thread;
        return thread;
    }

    public void waitUntilWaiting()
    {
        waitUntilWaiting( 0, TimeUnit.SECONDS );
    }
    
    public void waitUntilWaiting( long timeout, TimeUnit unit )
    {
        long end = timeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + unit.toMillis( timeout );
        Thread thread = getThread();
        while ( thread.getState() != Thread.State.WAITING || !executingCommand )
        {
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                // whatever
            }
            
            if ( System.currentTimeMillis() > end )
                throw new IllegalStateException( "The executor didn't wait inside an executing command for " + timeout + " " + unit );
        }
        for ( StackTraceElement e : thread.getStackTrace() )
        {
            System.out.println( e );
        }
    }

    private Thread getThread()
    {
        Thread thread = null;
        while (thread == null) thread = this.thread;
        return thread;
    }
    
    public void shutdown()
    {
        commandExecutor.shutdown();
        try
        {
            commandExecutor.awaitTermination( 1000, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {   // OK
        }
    }
}
