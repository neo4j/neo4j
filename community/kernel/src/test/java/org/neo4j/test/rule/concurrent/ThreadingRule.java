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
package org.neo4j.test.rule.concurrent;

import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.neo4j.function.FailableConsumer;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.test.ReflectionUtil;

import static org.neo4j.function.ThrowingPredicate.throwingPredicate;

public class ThreadingRule extends ExternalResource
{
    private ExecutorService executor;
    private static final FailableConsumer<Thread> NULL_CONSUMER = new FailableConsumer<Thread>()
    {
        @Override
        public void fail( Exception failure )
        {
        }

        @Override
        public void accept( Thread thread )
        {
        }
    };

    @Override
    protected void before()
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
        return executor.submit( task( function, function.toString(), parameter, NULL_CONSUMER ) );
    }

    public <FROM, TO, EX extends Exception> List<Future<TO>> multiple( int threads, ThrowingFunction<FROM,TO,EX> function, FROM parameter )
    {
        List<Future<TO>> result = new ArrayList<>( threads );
        for ( int i = 0; i < threads; i++ )
        {
            result.add( executor.submit( task(
                    function, function.toString() + ":task=" + i, parameter, NULL_CONSUMER ) ) );
        }
        return result;
    }

    public static <T> List<T> await( Iterable<Future<T>> futures ) throws InterruptedException, ExecutionException
    {
        List<T> result = futures instanceof Collection
                ? new ArrayList<>( ((Collection) futures).size() )
                : new ArrayList<>();
        List<Throwable> failures = null;
        for ( Future<T> future : futures )
        {
            try
            {
                result.add( future.get() );
            }
            catch ( ExecutionException e )
            {
                if ( failures == null )
                {
                    failures = new ArrayList<>();
                }
                failures.add( e.getCause() );
            }
        }
        if ( failures != null )
        {
            if ( failures.size() == 1 )
            {
                throw new ExecutionException( failures.get( 0 ) );
            }
            ExecutionException exception = new ExecutionException( null );
            for ( Throwable failure : failures )
            {
                exception.addSuppressed( failure );
            }
            throw exception;
        }
        return result;
    }

    public <FROM, TO, EX extends Exception> Future<TO> executeAndAwait(
            ThrowingFunction<FROM,TO,EX> function, FROM parameter, Predicate<Thread> threadCondition,
            long timeout, TimeUnit unit ) throws ExecutionException
    {
        FailableConcurrentTransfer<Thread> transfer = new FailableConcurrentTransfer<>();
        Future<TO> future = executor.submit( task( function, function.toString(), parameter, transfer ) );
        try
        {
            Predicates.awaitEx( transfer, throwingPredicate( threadCondition ), timeout, unit );
        }
        catch ( Exception e )
        {
            throw new ExecutionException( e );
        }
        return future;
    }

    private static <FROM, TO, EX extends Exception> Callable<TO> task(
            final ThrowingFunction<FROM,TO,EX> function, String name, final FROM parameter,
            final FailableConsumer<Thread> threadConsumer )
    {
        return () ->
        {
            Thread thread = Thread.currentThread();
            String previousName = thread.getName();
            thread.setName( name );
            threadConsumer.accept( thread );
            try
            {
                return function.apply( parameter );
            }
            catch ( Exception failure )
            {
                threadConsumer.fail( failure );
                throw failure;
            }
            finally
            {
                thread.setName( previousName );
            }
        };
    }

    /*Sample Stacktrace for method on owner*/
    public static Predicate<Thread> waitingWhileIn( final Class<?> owner, final String method )
    {
        return new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
            {
                ReflectionUtil.verifyMethodExists( owner, method );

                if ( thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING )
                {
                    for ( StackTraceElement element : thread.getStackTrace() )
                    {
                        if ( element.getClassName().equals( owner.getName() ) && element.getMethodName().equals( method ) )
                        {
                            return true;
                        }
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

    private static class FailableConcurrentTransfer<TYPE> implements FailableConsumer<TYPE>, ThrowingSupplier<TYPE, Exception>
    {
        private final CountDownLatch latch = new CountDownLatch( 1 );
        private TYPE value;
        private Exception failure;

        @Override
        public void accept( TYPE value )
        {
            this.value = value;
            latch.countDown();
        }

        @Override
        public void fail( Exception failure )
        {
            this.failure = failure;
            latch.countDown();
        }

        @Override
        public TYPE get() throws Exception
        {
            latch.await();
            if ( failure != null )
            {
                throw failure;
            }
            return value;
        }

        @Override
        public String toString()
        {
            return String.format( "ConcurrentTransfer{%s}", latch.getCount() == 1 ? "<waiting>" : value );
        }
    }
}
