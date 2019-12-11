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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.helpers.collection.LongRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.consistency.newchecker.ParallelExecution.NOOP_EXCEPTION_HANDLER;

class ParallelExecutionTest
{
    private final int NUM_THREADS = 3;

    @Test
    void shouldRethrowException()
    {
        ParallelExecution execution = new ParallelExecution( NUM_THREADS, NOOP_EXCEPTION_HANDLER, 100 );
        assertThrows( Exception.class, () -> execution.run( getClass().getSimpleName(), () ->
        {
            throw new Exception();
        } ) );
    }

    @Test
    void exceptionHandlerShouldSeeBothCheckedAndUnchecked()
    {
        AtomicInteger counter = new AtomicInteger( 0 );
        ParallelExecution execution = new ParallelExecution( NUM_THREADS, e -> counter.incrementAndGet(), 100 );

        ParallelExecution.ThrowingRunnable checkedException = () ->
        {
            throw new Exception();
        };
        ParallelExecution.ThrowingRunnable uncheckedException = () ->
        {
            throw new RuntimeException();
        };
        ParallelExecution.ThrowingRunnable assertion = () ->
        {
            assert false;
        };
        ParallelExecution.ThrowingRunnable oom = () ->
        {
            throw new OutOfMemoryError();
        };
        try
        {
            execution.run( getClass().getSimpleName(), checkedException, uncheckedException, assertion, oom );
        }
        catch ( Exception e )
        {
            //expected
        }

        assertEquals( 4, counter.get() );
    }

    @Test
    void shouldChainAllExceptions()
    {
        ParallelExecution execution = new ParallelExecution( NUM_THREADS, NOOP_EXCEPTION_HANDLER, 100 );
        Exception e1 = new Exception( "A" );
        Exception e2 = new Exception( "B" );
        Exception e3 = new Exception( "C" );
        Exception exception = assertThrows( Exception.class, () -> execution.run( getClass().getSimpleName(), () ->
        {
            throw e1;
        }, () ->
        {
            throw e2;
        }, () ->
        {
            throw e3;
        } ) );
        Throwable[] suppressed = exception.getSuppressed();
        List<String> messages = List.of( exception.getCause().getMessage(), suppressed[0].getCause().getMessage(), suppressed[1].getCause().getMessage() );

        assertThat( messages ).contains( "A", "B", "C" );
    }

    @Test
    void shouldPartitionIdRanges()
    {
        // given
        ParallelExecution execution = new ParallelExecution( 10, NOOP_EXCEPTION_HANDLER, 145 );
        ParallelExecution.RangeOperation rangeOperation = mock( ParallelExecution.RangeOperation.class );

        // when
        execution.partition( LongRange.range( 0, 470 ), rangeOperation );

        // then
        verify( rangeOperation ).operation( 0, 145, false );
        verify( rangeOperation ).operation( 145, 290, false );
        verify( rangeOperation ).operation( 290, 435, false );
        verify( rangeOperation ).operation( 435, 470, true );
        verifyNoMoreInteractions( rangeOperation );
    }

    @Test
    void shouldRunAllJobsConcurrently() throws Exception
    {
        // given
        ParallelExecution execution = new ParallelExecution( 2, NOOP_EXCEPTION_HANDLER, 100 );
        ParallelExecution.ThrowingRunnable[] blockingJobs = new ParallelExecution.ThrowingRunnable[execution.getNumberOfThreads() + 2];
        CountDownLatch latch = new CountDownLatch( blockingJobs.length );
        Arrays.fill( blockingJobs, (ParallelExecution.ThrowingRunnable) () ->
        {
            latch.countDown();
            latch.await();
        } );

        // when
        execution.runAll( "test", blockingJobs );

        // then
        assertEquals( 0, latch.getCount() );
    }

    @Test
    void shouldRunJobs() throws Exception
    {
        // given
        ParallelExecution execution = new ParallelExecution( 2, NOOP_EXCEPTION_HANDLER, 100 );
        ParallelExecution.ThrowingRunnable[] blockingJobs = new ParallelExecution.ThrowingRunnable[execution.getNumberOfThreads() * 5];
        Set<Thread> threads = new CopyOnWriteArraySet<>();
        Arrays.fill( blockingJobs, (ParallelExecution.ThrowingRunnable) () ->
        {
            Thread.sleep( 10 );
            threads.add( Thread.currentThread() );
        } );

        // when
        execution.run( "test", blockingJobs );

        // then
        assertEquals( 2, threads.size() );
    }
}
