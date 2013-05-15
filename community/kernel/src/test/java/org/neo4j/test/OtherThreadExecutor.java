/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger.LineLogger;

/**
 * Executes {@link WorkerCommand}s in another thread. Very useful for writing
 * tests which handles two simultaneous transactions and interleave them,
 * f.ex for testing locking and data visibility.
 * 
 * @author Mattias Persson
 *
 * @param <T>
 */
public class OtherThreadExecutor<T> implements ThreadFactory, Visitor<LineLogger, RuntimeException>
{
    private final ExecutorService commandExecutor = newSingleThreadExecutor( this );
    protected final T state;
    private volatile Thread thread;
    private volatile ExecutionState executionState;
    private final String name;
    private final long timeout;
    private Exception lastExecutionTrigger;
    
    private static enum ExecutionState
    {
        REQUESTED_EXECUTION,
        EXECUTING,
        EXECUTED
    }

    public OtherThreadExecutor( String name, T initialState )
    {
        this( name, 10, SECONDS, initialState );
    }
    
    public OtherThreadExecutor( String name, long timeout, TimeUnit unit, T initialState )
    {
        this.name = name;
        this.state = initialState;
        this.timeout = MILLISECONDS.convert( timeout, unit );
    }

    public <R> Future<R> executeDontWait( final WorkerCommand<T, R> cmd ) throws Exception
    {
        lastExecutionTrigger = new Exception();
        executionState = ExecutionState.REQUESTED_EXECUTION;
        return commandExecutor.submit( new Callable<R>()
        {
            @Override
            public R call() throws Exception
            {
                executionState = ExecutionState.EXECUTING;
                try
                {
                    return cmd.doWork( state );
                }
                finally
                {
                    executionState = ExecutionState.EXECUTED;
                }
            }
        } );
    }
    
    public <R> R execute( WorkerCommand<T, R> cmd ) throws Exception
    {
        return executeDontWait( cmd ).get();
    }

    public <R> R execute( WorkerCommand<T, R> cmd, long timeout, TimeUnit unit ) throws Exception
    {
        Future<R> future = executeDontWait( cmd );
        boolean success = false;
        try
        {
            R result = future.get( timeout, unit );
            success = true;
            return result;
        }
        finally
        {
            if ( !success )
                future.cancel( true );
        }
    }
    
    public <R> R awaitFuture( Future<R> future ) throws InterruptedException, ExecutionException, TimeoutException
    {
        return future.get( timeout, MILLISECONDS );
    }
    
    public interface WorkerCommand<T, R>
    {
        R doWork( T state ) throws Exception;
    }

    @Override
    public Thread newThread( Runnable r )
    {
        Thread thread = new Thread( r, getClass().getName() + ":" + name )
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
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ":" + name;
    }

    public void waitUntilWaiting() throws TimeoutException
    {
        waitUntilThreadState( Thread.State.WAITING );
    }
    
    public void waitUntilThreadState( Thread.State... possibleStates ) throws TimeoutException
    {
        Set<Thread.State> stateSet = new HashSet<Thread.State>( asList( possibleStates ) );
        long end = System.currentTimeMillis() + timeout;
        Thread thread = getThread();
        Set<Thread.State> seenStates = new HashSet<Thread.State>();
        for ( Thread.State state;
              !stateSet.contains( (state = thread.getState()) ) ||
                      executionState == ExecutionState.REQUESTED_EXECUTION; )
        {
            seenStates.add( state );
            try
            {
                Thread.sleep( 1 );
            }
            catch ( InterruptedException e )
            {
                // whatever
            }
            
            if ( System.currentTimeMillis() > end )
            {
                throw new TimeoutException( "The executor didn't enter any of states " +
                        Arrays.toString( possibleStates ) + " inside an executing command for " +
                        timeout + " ms. Seen states: " + seenStates );
            }
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
    
    @Override
    public boolean visit( LineLogger logger )
    {
        logger.logLine( getClass().getName() + ", " + this + " state:" + state + " thread:" + thread +
                " execution:" + executionState );
        if ( thread != null )
        {
            logger.logLine( "Thread state:" + thread.getState() );
            logger.logLine( "" );
            for ( StackTraceElement element : thread.getStackTrace() )
                logger.logLine( element.toString() );
        }
        else
        {
            logger.logLine( "No operations performed yet, so no thread" );
        }
        if ( lastExecutionTrigger != null )
        {
            logger.logLine( "" );
            logger.logLine( "Last execution triggered from:" );
            for ( StackTraceElement element : lastExecutionTrigger.getStackTrace() )
                logger.logLine( element.toString() );
        }
        return true;
    }
}
