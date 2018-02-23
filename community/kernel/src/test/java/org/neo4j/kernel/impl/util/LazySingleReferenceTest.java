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
package org.neo4j.kernel.impl.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class LazySingleReferenceTest
{
    @Test
    public void shouldOnlyAllowSingleThreadToInitialize() throws Exception
    {
        // GIVEN
        final CountDownLatch latch = new CountDownLatch( 1 );
        final AtomicInteger initCalls = new AtomicInteger();
        LazySingleReference<Integer> ref = new LazySingleReference<Integer>()
        {
            @Override
            protected Integer create()
            {
                awaitLatch( latch );
                return initCalls.incrementAndGet();
            }
        };
        Future<Integer> t1Evaluate = t1.executeDontWait( evaluate( ref ) );
        t1.waitUntilWaiting();

        // WHEN
        Future<Integer> t2Evaluate = t2.executeDontWait( evaluate( ref ) );
        t2.waitUntilBlocked();
        latch.countDown();
        int e1 = t1Evaluate.get();
        int e2 = t2Evaluate.get();

        // THEN
        assertEquals( 1, e1, "T1 evaluation" );
        assertEquals( 1, e2, "T2 evaluation" );
    }

    @Test
    public void shouldMutexAccessBetweenInvalidateAndinstance() throws Exception
    {
        // GIVEN
        final CountDownLatch latch = new CountDownLatch( 1 );
        final AtomicInteger initCalls = new AtomicInteger();
        LazySingleReference<Integer> ref = new LazySingleReference<Integer>()
        {
            @Override
            protected Integer create()
            {
                awaitLatch( latch );
                return initCalls.incrementAndGet();
            }
        };
        Future<Integer> t1Evaluate = t1.executeDontWait( evaluate( ref ) );
        t1.waitUntilWaiting();

        // WHEN
        Future<Void> t2Invalidate = t2.executeDontWait( invalidate( ref ) );
        t2.waitUntilBlocked();
        latch.countDown();
        int e = t1Evaluate.get();
        t2Invalidate.get();

        // THEN
        assertEquals( 1, e, "Evaluation" );
    }

    @Test
    public void shouldInitializeAgainAfterInvalidated()
    {
        // GIVEN
        final AtomicInteger initCalls = new AtomicInteger();
        LazySingleReference<Integer> ref = new LazySingleReference<Integer>()
        {
            @Override
            protected Integer create()
            {
                return initCalls.incrementAndGet();
            }
        };
        assertEquals( 1, ref.get().intValue(), "First evaluation" );

        // WHEN
        ref.invalidate();
        int e2 = ref.get();

        // THEN
        assertEquals( 2, e2, "Second evaluation" );
    }

    @Test
    public void shouldRespondToIsInitialized()
    {
        // GIVEN
        LazySingleReference<Integer> ref = new LazySingleReference<Integer>()
        {
            @Override
            protected Integer create()
            {
                return 1;
            }
        };

        // WHEN
        boolean firstResult = ref.isCreated();
        ref.get();
        boolean secondResult = ref.isCreated();
        ref.invalidate();
        boolean thirdResult = ref.isCreated();
        ref.get();
        boolean fourthResult = ref.isCreated();

        // THEN
        assertFalse( firstResult, "Should not start off as initialized" );
        assertTrue( secondResult, "Should be initialized after an evaluation" );
        assertFalse( thirdResult, "Should not be initialized after invalidated" );
        assertTrue( fourthResult, "Should be initialized after a re-evaluation" );
    }

    private OtherThreadExecutor<Void> t1;
    private OtherThreadExecutor<Void> t2;

    @BeforeEach
    public void before()
    {
        t1 = new OtherThreadExecutor<>( "T1", null );
        t2 = new OtherThreadExecutor<>( "T2", null );
    }

    @AfterEach
    public void after()
    {
        t2.close();
        t1.close();
    }

    private WorkerCommand<Void,Integer> evaluate( final LazySingleReference<Integer> ref )
    {
        return state -> ref.get();
    }

    private WorkerCommand<Void,Void> invalidate( final LazySingleReference<Integer> ref )
    {
        return state ->
        {
            ref.invalidate();
            return null;
        };
    }
}
