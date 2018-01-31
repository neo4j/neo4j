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
package org.neo4j.bolt.runtime;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.neo4j.function.Predicates;
import org.neo4j.logging.NullLog;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CachedThreadPoolExecutorFactoryTest
{
    private final ExecutorFactory factory = new CachedThreadPoolExecutorFactory( NullLog.getInstance() );
    private ExecutorService executorService;

    @After
    public void cleanup()
    {
        if ( executorService != null && !executorService.isTerminated() )
        {
            factory.destroy( executorService );
        }
    }

    @Test
    public void createShouldCreateExecutor()
    {
        executorService = factory.create( 0, 1, Duration.ZERO, newThreadFactory() );

        assertNotNull( executorService );
        assertFalse( executorService.isShutdown() );
        assertFalse( executorService.isTerminated() );
    }

    @Test
    public void createShouldNotCreateExecutorWhenCorePoolSizeIsNegative()
    {
        try
        {
            factory.create( -1, 10, Duration.ZERO, newThreadFactory() );
            fail( "should throw exception" );
        }
        catch ( IllegalArgumentException ex )
        {
            //
        }
    }

    @Test
    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsNegative()
    {
        try
        {
            factory.create( 0, -1, Duration.ZERO, newThreadFactory() );
            fail( "should throw exception" );
        }
        catch ( IllegalArgumentException ex )
        {
            //
        }
    }

    @Test
    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsZero()
    {
        try
        {
            factory.create( 0, 0, Duration.ZERO, newThreadFactory() );
            fail( "should throw exception" );
        }
        catch ( IllegalArgumentException ex )
        {
            //
        }
    }

    @Test
    public void createShouldNotCreateExecutorWhenMaxPoolSizeIsLessThanCorePoolSize()
    {
        try
        {
            factory.create( 10, 5, Duration.ZERO, newThreadFactory() );
            fail( "should throw exception" );
        }
        catch ( IllegalArgumentException ex )
        {
            //
        }
    }

    @Test
    public void destroyShouldDestroyExecutor()
    {
        executorService = factory.create( 0, 1, Duration.ZERO, newThreadFactory() );

        factory.destroy( executorService );

        assertTrue( executorService.isShutdown() );
        assertTrue( executorService.isTerminated() );
    }

    @Test
    public void destroyShouldNotDestroyExecutorsCreatedElsewhere()
    {
        ExecutorService otherExecutorService = Executors.newSingleThreadExecutor();

        try
        {
            factory.destroy( otherExecutorService );
            fail( "should throw exception" );
        }
        catch ( IllegalArgumentException ex )
        {
            //
        }
    }

    @Test
    public void createdExecutorShouldExecuteSubmittedTasks() throws Exception
    {
        AtomicBoolean exitCondition = new AtomicBoolean( false );
        AtomicInteger threadCounter = new AtomicInteger( 0 );

        executorService = factory.create( 0, 1, Duration.ZERO, newThreadFactoryWithCounter( threadCounter ) );

        assertNotNull( executorService );
        assertEquals( 0, threadCounter.get() );

        Future task1 = executorService.submit( newInfiniteWaitingRunnable( exitCondition ) );
        assertEquals( 1, threadCounter.get() );

        exitCondition.set( true );

        assertNull( task1.get( 1, MINUTES ) );
        assertTrue( task1.isDone() );
        assertFalse( task1.isCancelled() );
    }

    @Test
    public void createdExecutorShouldFavorPoolSizes()
    {
        AtomicBoolean exitCondition = new AtomicBoolean( false );
        AtomicInteger threadCounter = new AtomicInteger( 0 );

        executorService = factory.create( 0, 5, Duration.ZERO, newThreadFactoryWithCounter( threadCounter ) );

        assertNotNull( executorService );
        assertEquals( 0, threadCounter.get() );

        for ( int i = 0; i < 5; i++ )
        {
            executorService.submit( newInfiniteWaitingRunnable( exitCondition ) );
            assertEquals( i + 1, threadCounter.get() );
        }

        try
        {
            executorService.submit( newInfiniteWaitingRunnable( exitCondition ) );
            fail( "should throw exception" );
        }
        catch ( RejectedExecutionException ex )
        {
            //
        }
    }

    @Test
    public void createdExecutorShouldPrecreateCoreThreads()
    {
        AtomicInteger threadCounter = new AtomicInteger( 0 );

        executorService = factory.create( 5, 10, Duration.ZERO, newThreadFactoryWithCounter( threadCounter ) );

        assertNotNull( executorService );
        assertEquals( 5, threadCounter.get() );
    }

    private static Runnable newInfiniteWaitingRunnable( AtomicBoolean exitCondition )
    {
        return () -> Predicates.awaitForever( () -> !Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS );
    }

    private static ThreadFactory newThreadFactory()
    {
        return Executors.defaultThreadFactory();
    }

    private static ThreadFactory newThreadFactoryWithCounter( AtomicInteger counter )
    {
        return job ->
        {
            counter.incrementAndGet();
            return Executors.defaultThreadFactory().newThread( job );
        };
    }
}
