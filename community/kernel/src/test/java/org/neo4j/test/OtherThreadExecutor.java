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
package org.neo4j.test;

import java.io.Closeable;
import java.io.PrintStream;
import java.lang.Thread.State;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.function.Predicate;
import org.neo4j.logging.Logger;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Executes {@link WorkerCommand}s in another thread. Very useful for writing
 * tests which handles two simultaneous transactions and interleave them,
 * f.ex for testing locking and data visibility.
 *
 * @param <T>
 * @author Mattias Persson
 */
public class OtherThreadExecutor<T> implements ThreadFactory, Closeable
{
    private final ExecutorService commandExecutor = newSingleThreadExecutor( this );
    protected final T state;
    private volatile Thread thread;
    private volatile ExecutionState executionState;
    private final String name;
    private final long timeout;
    private Exception lastExecutionTrigger;

    private static final class AnyThreadState implements Predicate<Thread>
    {
        private final Set<State> possibleStates;
        private final Set<Thread.State> seenStates = new HashSet<>();

        private AnyThreadState( State... possibleStates )
        {
            this.possibleStates = new HashSet<>( asList( possibleStates ) );
        }

        @Override
        public boolean test( Thread thread )
        {
            State threadState = thread.getState();
            seenStates.add( threadState );
            return possibleStates.contains( threadState );
        }

        @Override
        public String toString()
        {
            return "Any of thread states " + possibleStates + ", but saw " + seenStates;
        }
    }

    public static Predicate<Thread> anyThreadState( State... possibleStates )
    {
        return new AnyThreadState( possibleStates );
    }

    public Predicate<Thread> orExecutionCompleted( final Predicate<Thread> actual )
    {
        return new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
            {
                return actual.test( thread ) || executionState == ExecutionState.EXECUTED;
            }

            @Override
            public String toString()
            {
                return "(" + actual.toString() + ") or execution completed.";
            }
        };
    }

    private enum ExecutionState
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

    public <R> Future<R> executeDontWait( final WorkerCommand<T, R> cmd )
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
            awaitStartExecuting();
            R result = future.get( timeout, unit );
            success = true;
            return result;
        }
        finally
        {
            if ( !success )
            {
                future.cancel( true );
            }
        }
    }

    void awaitStartExecuting() throws InterruptedException
    {
        while ( executionState == ExecutionState.REQUESTED_EXECUTION )
        {
            Thread.sleep( 10 );
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
        Thread thread = this.thread;
        return format( "%s[%s,state=%s]", getClass().getSimpleName(), name,
                       thread == null ? "dead" : thread.getState() );
    }

    public WaitDetails waitUntilWaiting() throws TimeoutException
    {
        return waitUntilThreadState( Thread.State.WAITING, Thread.State.TIMED_WAITING );
    }

    public WaitDetails waitUntilBlocked() throws TimeoutException
    {
        return waitUntilThreadState( Thread.State.BLOCKED );
    }

    public WaitDetails waitUntilThreadState( final Thread.State... possibleStates ) throws TimeoutException
    {
        return waitUntil( new AnyThreadState( possibleStates ) );
    }

    public WaitDetails waitUntil( Predicate<Thread> condition ) throws TimeoutException
    {
        long end = System.currentTimeMillis() + timeout;
        Thread thread = getThread();
        while ( !condition.test( thread ) || executionState == ExecutionState.REQUESTED_EXECUTION )
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
            {
                throw new TimeoutException( "The executor didn't meet condition '" + condition +
                        "' inside an executing command for " + timeout + " ms" );
            }
        }

        if ( executionState == ExecutionState.EXECUTED )
        {
            throw new IllegalStateException( "Would have wanted " + thread + " to wait for " + condition +
                    " but that never happened within the duration of executed task" );
        }

        return new WaitDetails( thread.getStackTrace() );
    }

    public static class WaitDetails
    {
        private final StackTraceElement[] stackTrace;

        public WaitDetails( StackTraceElement[] stackTrace )
        {
            this.stackTrace = stackTrace;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            for ( StackTraceElement element : stackTrace )
            {
                builder.append( format( element.toString() + "%n" ) );
            }
            return builder.toString();
        }

        public boolean isAt( Class<?> clz, String method )
        {
            for ( StackTraceElement element : stackTrace )
            {
                if ( element.getClassName().equals( clz.getName() ) && element.getMethodName().equals( method ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    public Thread.State state()
    {
        return thread.getState();
    }

    private Thread getThread()
    {
        Thread thread = null;
        while ( thread == null )
        {
            thread = this.thread;
        }
        return thread;
    }

    @Override
    public void close()
    {
        commandExecutor.shutdown();
        try
        {
            commandExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            // shutdownNow() will interrupt running tasks if necessary
        }
        if ( ! commandExecutor.isTerminated() )
        {
            commandExecutor.shutdownNow();
        }
    }

    public void dump( Logger logger )
    {
        logger.log( getClass().getName() + ", " + this + " state:" + state + " thread:" + thread +
                " execution:" + executionState );
        if ( thread != null )
        {
            logger.log( "Thread state:" + thread.getState() );
            logger.log( "" );
            for ( StackTraceElement element : thread.getStackTrace() )
            {
                logger.log( element.toString() );
            }
        }
        else
        {
            logger.log( "No operations performed yet, so no thread" );
        }
        if ( lastExecutionTrigger != null )
        {
            logger.log( "" );
            logger.log( "Last execution triggered from:" );
            for ( StackTraceElement element : lastExecutionTrigger.getStackTrace() )
            {
                logger.log( element.toString() );
            }
        }
    }

    public void interrupt()
    {
        if(thread != null)
        {
            thread.interrupt();
        }
    }

    void printStackTrace( PrintStream out )
    {
        Thread thread = getThread();
        out.println( thread );
        for ( StackTraceElement trace : thread.getStackTrace() )
        {
            out.println( "\tat " + trace );
        }
    }
}
