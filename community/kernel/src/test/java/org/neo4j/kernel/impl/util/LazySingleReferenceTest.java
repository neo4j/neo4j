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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        assertEquals( "T1 evaluation", 1, e1 );
        assertEquals( "T2 evaluation", 1, e2 );
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
        assertEquals( "Evaluation", 1, e );
    }
    
    @Test
    public void shouldInitializeAgainAfterInvalidated() throws Exception
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
        assertEquals( "First evaluation", 1, ref.get().intValue() );
        
        // WHEN
        ref.invalidate();
        int e2 = ref.get();
        
        // THEN
        assertEquals( "Second evaluation", 2, e2 );
    }
    
    @Test
    public void shouldRespondToIsInitialized() throws Exception
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
        assertFalse( "Should not start off as initialized", firstResult );
        assertTrue( "Should be initialized after an evaluation", secondResult );
        assertFalse( "Should not be initialized after invalidated", thirdResult );
        assertTrue( "Should be initialized after a re-evaluation", fourthResult );
    }
    
    private OtherThreadExecutor<Void> t1, t2;
    
    @Before
    public void before()
    {
        t1 = new OtherThreadExecutor<>( "T1", null );
        t2 = new OtherThreadExecutor<>( "T2", null );
    }

    @After
    public void after()
    {
        t2.close();
        t1.close();
    }

    private WorkerCommand<Void,Integer> evaluate( final LazySingleReference<Integer> ref )
    {
        return new WorkerCommand<Void,Integer>()
        {
            @Override
            public Integer doWork( Void state ) throws Exception
            {
                return ref.get();
            }
        };
    }
    
    private WorkerCommand<Void,Void> invalidate( final LazySingleReference<Integer> ref )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                ref.invalidate();
                return null;
            }
        };
    }
}
