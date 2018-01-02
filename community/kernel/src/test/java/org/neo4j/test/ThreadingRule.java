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

import org.junit.rules.ExternalResource;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.helpers.ConcurrentTransfer;

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

    public <FROM, TO, EX extends Exception> Future<TO> execute( ThrowingFunction<FROM,TO,EX> function, FROM parameter )
    {
        return executor.submit( task( Barrier.NONE, function, parameter, Consumers.<Thread>noop() ) );
    }

    public <FROM, TO, EX extends Exception> Future<TO> executeAndAwait(
            ThrowingFunction<FROM,TO,EX> function, FROM parameter, Predicate<Thread> threadCondition,
            long timeout, TimeUnit unit ) throws TimeoutException, InterruptedException
    {
        ConcurrentTransfer<Thread> threadTransfer = new ConcurrentTransfer<>();
        Future<TO> future = executor.submit( task( Barrier.NONE, function, parameter, threadTransfer ) );
        Predicates.await( threadTransfer, threadCondition, timeout, unit );
        return future;
    }

    private static <FROM, TO, EX extends Exception> Callable<TO> task(
            final Barrier barrier, final ThrowingFunction<FROM,TO,EX> function, final FROM parameter,
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

    public static Predicate<Thread> waitingWhileIn( final Class<?> owner, final String method )
    {
        return new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
            {
                ReflectionUtil.verifyMethodExists( owner, method );

                if ( thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING )
                {
                    return false;
                }
                for ( StackTraceElement element : thread.getStackTrace() )
                {
                    if ( element.getClassName().equals( owner.getName() ) && element.getMethodName().equals( method ) )
                    {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString()
            {
                return String.format( "Predicate[Thread.state=WAITING && thread.getStackTrace() contains %s.%s()]",
                        owner.getName(), method );
            }
        };
    }
}
