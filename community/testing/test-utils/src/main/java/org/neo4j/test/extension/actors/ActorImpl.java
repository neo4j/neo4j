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
package org.neo4j.test.extension.actors;

import java.lang.reflect.Executable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

class ActorImpl implements Actor
{
    private static final FutureTask<Void> STOP_SIGNAL = new FutureTask<>( () -> null );
    private final LinkedTransferQueue<FutureTask<?>> queue;
    private final Thread thread;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile boolean stopped;
    private volatile boolean executing;

    ActorImpl( ThreadGroup threadGroup, String name )
    {
        queue = new LinkedTransferQueue<>();
        thread = new Thread( threadGroup, this::runActor, name );
    }

    private <T> void enqueue( FutureTask<T> task )
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Test actor is stopped: " + thread );
        }
        queue.offer( task );
        if ( !started.get() && started.compareAndSet( false, true ) )
        {
            thread.start();
        }
    }

    private void runActor()
    {
        try
        {
            FutureTask<?> task;
            while ( !stopped && (task = queue.take()) != STOP_SIGNAL )
            {
                try
                {
                    executing = true;
                    task.run();
                }
                finally
                {
                    executing = false;
                }
            }
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }

    public void stop()
    {
        stopped = true;
        queue.offer( STOP_SIGNAL );
    }

    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    public void join() throws InterruptedException
    {
        Thread.interrupted(); // Clear interrupted flag.
        thread.join();
    }

    @Override
    public <T> Future<T> submit( Callable<T> callable )
    {
        FutureTask<T> task = new FutureTask<>( callable );
        enqueue( task );
        return task;
    }

    @Override
    public <T> Future<T> submit( Runnable runnable, T result )
    {
        FutureTask<T> task = new FutureTask<>( runnable, result );
        enqueue( task );
        return task;
    }

    @Override
    public Future<Void> submit( Runnable runnable )
    {
        return submit( runnable, null );
    }

    @Override
    public void untilWaiting() throws InterruptedException
    {
        do
        {
            Thread.State state = thread.getState();
            boolean executing = this.executing;
            if ( executing && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) )
            {
                return;
            }
            if ( state == Thread.State.TERMINATED )
            {
                throw new AssertionError( "Actor thread " + thread.getName() + " has terminated." );
            }
            if ( state == Thread.State.NEW )
            {
                throw new IllegalStateException( "Actor thread " + thread.getName() + " has not yet started." );
            }
            if ( queue.hasWaitingConsumer() && queue.isEmpty() )
            {
                throw new IllegalStateException( "There are no tasks running or queued up that we can wait for." );
            }
            if ( Thread.interrupted() )
            {
                throw new InterruptedException();
            }
            Thread.onSpinWait();
        }
        while ( true );
    }

    @Override
    public void untilWaitingIn( Executable constructorOrMethod ) throws InterruptedException
    {
        untilWaitingIn( methodPredicate( constructorOrMethod ) );
    }

    @Override
    public void untilWaitingIn( String methodName ) throws InterruptedException
    {
        untilWaitingIn( methodPredicate( methodName ) );
    }

    private void untilWaitingIn( Predicate<StackTraceElement> predicate ) throws InterruptedException
    {
        do
        {
            untilWaiting();
            if ( isIn( predicate ) )
            {
                return;
            }
            Thread.sleep( 1 );
        }
        while ( true );
    }

    @Override
    public void untilThreadState( Thread.State... states )
    {
        EnumSet<Thread.State> set = EnumSet.copyOf( Arrays.asList( states ) );
        do
        {
            if ( !queue.hasWaitingConsumer() )
            {
                Thread.State state = thread.getState();
                if ( set.contains( state ) )
                {
                    return;
                }
            }
            Thread.onSpinWait();
        }
        while ( true );
    }

    @Override
    public void interrupt()
    {
        thread.interrupt();
    }

    private Predicate<StackTraceElement> methodPredicate( Executable constructorOrMethod )
    {
        String targetMethodName = constructorOrMethod.getName();
        String targetClassName = constructorOrMethod.getDeclaringClass().getName();
        return element ->
            element.getMethodName().equals( targetMethodName ) && element.getClassName().equals( targetClassName );
    }

    private Predicate<StackTraceElement> methodPredicate( String methodName )
    {
        return element -> element.getMethodName().equals( methodName );
    }

    private boolean isIn( Predicate<StackTraceElement> predicate )
    {
        StackTraceElement[] stackTrace = thread.getStackTrace();
        for ( StackTraceElement element : stackTrace )
        {
            if ( predicate.test( element ) )
            {
                return true;
            }
        }
        return false;
    }
}
