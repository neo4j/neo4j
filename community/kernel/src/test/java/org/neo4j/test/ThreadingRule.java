/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.rules.ExternalResource;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.function.Consumer;
import org.neo4j.function.RawFunction;
import org.neo4j.helpers.Cancelable;
import org.neo4j.helpers.CancellationRequest;
import org.neo4j.helpers.ConcurrentTransfer;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;

import static java.util.Objects.requireNonNull;
import static org.neo4j.function.Functions.swallow;

public class ThreadingRule extends ExternalResource
{
    private ExecutorService executor;

    @Override
    protected void before() throws Throwable
    {
        executor = Executors.newCachedThreadPool();
    }

    @Override
    protected void after()
    {
        try
        {
            executor.shutdownNow();
            executor.awaitTermination( 1, TimeUnit.MINUTES );
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        finally
        {
            executor = null;
        }
    }

    public <FROM, TO, EX extends Exception> Future<TO> execute( RawFunction<FROM,TO,EX> function, FROM parameter )
    {
        return executor.submit( task( Barrier.NONE, function, parameter, swallow( Thread.class ) ) );
    }

    public <FROM, TO, EX extends Exception> Future<TO> executeAfter(
            Barrier barrier, RawFunction<FROM,TO,EX> function, FROM parameter )
    {
        return executor.submit( task( barrier, function, parameter, swallow( Thread.class ) ) );
    }

    public <FROM, TO, EX extends Exception> Future<TO> executeAndAwait(
            RawFunction<FROM,TO,EX> function, FROM parameter, Predicate<Thread> threadCondition,
            long timeout, TimeUnit unit ) throws TimeoutException, InterruptedException
    {
        ConcurrentTransfer<Thread> threadTransfer = new ConcurrentTransfer<>();
        Future<TO> future = executor.submit( task( Barrier.NONE, function, parameter, threadTransfer ) );
        Predicates.await( threadTransfer, threadCondition, timeout, unit );
        return future;
    }

    public Cancelable threadBlockMonitor( Thread thread, Runnable action )
    {
        CancellationHandle cancellation = new CancellationHandle();
        executor.submit( new ThreadBlockMonitor( cancellation,
                                                 requireNonNull( thread, "thread" ),
                                                 requireNonNull( action, "action" ) ) );
        return cancellation;
    }

    private static <FROM, TO, EX extends Exception> Callable<TO> task(
            final Barrier barrier, final RawFunction<FROM,TO,EX> function, final FROM parameter,
            final Consumer<Thread> threadConsumer )
    {
        return new Callable<TO>()
        {
            @Override
            public TO call() throws Exception
            {
                Thread thread = Thread.currentThread();
                String name = thread.getName();
                thread.setName( function.toString() );
                threadConsumer.accept( thread );
                barrier.reached();
                try
                {
                    return function.apply( parameter );
                }
                finally
                {
                    thread.setName( name );
                }
            }
        };
    }

    public static Predicate<Thread> stackTracePredicate( final int depth, final Class<?> owner, final String method )
    {
        return new Predicate<Thread>()
        {
            @Override
            public boolean accept( Thread thread )
            {
                StackTraceElement[] stackTrace = thread.getStackTrace();
                return stackTrace.length > depth &&
                       stackTrace[depth].getClassName().equals( owner.getName() ) &&
                       stackTrace[depth].getMethodName().equals( method );
            }

            @Override
            public String toString()
            {
                return String.format( "Predicate[thread.getStackTrace()[%s] == %s.%s()]",
                        depth, owner.getName(), method );
            }
        };
    }

    private static class CancellationHandle implements Cancelable, CancellationRequest
    {
        private volatile boolean cancelled = false;

        @Override
        public boolean cancellationRequested()
        {
            return cancelled;
        }

        public void cancel()
        {
            cancelled = true;
        }
    }

    private static class ThreadBlockMonitor implements Runnable
    {
        private final CancellationRequest cancellation;
        private final Thread thread;
        private final Runnable action;

        public ThreadBlockMonitor( CancellationRequest cancellation, Thread thread, Runnable action )
        {
            this.cancellation = cancellation;
            this.thread = thread;
            this.action = action;
        }

        @Override
        public void run()
        {
            StackTraceElement[] lastTrace = null;
            Thread.State lastState = null;
            do
            {
                Thread.State state = thread.getState();
                switch ( state )
                {
                case BLOCKED:
                case WAITING:
                case TIMED_WAITING:
                    StackTraceElement[] trace = thread.getStackTrace();
                    if ( trace[0].isNativeMethod() &&
                         Thread.class.getName().equals( trace[0].getClassName() ) &&
                         "sleep".equals( trace[0].getMethodName() ) )
                    {
                        break; // don't regard Thread.sleep() as being blocked
                    }
                    if ( lastState == state && Arrays.equals( trace, lastTrace ) )
                    {
                        action.run();
                        return;
                    }
                    lastTrace = trace;
                    break;
                default:
                    lastTrace = null;
                }
                lastState = state;
                try
                {
                    Thread.sleep( 500 );
                }
                catch ( InterruptedException e )
                {
                    return;
                }
            }
            while ( !cancellation.cancellationRequested() );
        }
    }
}
