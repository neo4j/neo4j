/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.unsafe.impl.batchimport.staging.Configuration.DEFAULT;

public class ForkedProcessorStepTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();
    @Rule
    public final OtherThreadRule<Void> t3 = new OtherThreadRule<>();
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldProcessBatchBySingleThread() throws Exception
    {
        // GIVEN
        SimpleStageControl control = new SimpleStageControl();
        AtomicReference<Object> processed = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch( 1 );
        try ( Step<Object> step = new ForkedProcessorStep<Object>( control, "Test", DEFAULT, 1 )
        {
            @Override
            protected void forkedProcess( int id, int processors, Object batch )
            {
                try
                {
                    assertEquals( 0, id );
                    assertEquals( 1, processors );
                    processed.set( batch );
                }
                finally
                {
                    latch.countDown();
                }
            }
        } )
        {
            control.steps( step );
            step.start( Step.ORDER_SEND_DOWNSTREAM );

            // WHEN
            Object expectedBatch = new Object();
            step.receive( 0, expectedBatch );

            // THEN
            latch.await();
            assertSame( expectedBatch, processed.get() );
            control.assertHealthy();
        }
    }

    @Test
    public void shouldProcessBatchByMultipleThreads() throws Exception
    {
        // GIVEN
        SimpleStageControl control = new SimpleStageControl();
        int threadCount = 10;
        CountDownLatch latch = new CountDownLatch( threadCount );
        Object expectedBatch = new Object();
        try ( Step<Object> step = new ForkedProcessorStep<Object>( control, "Test", DEFAULT, threadCount )
        {
            @Override
            protected void forkedProcess( int id, int processors, Object batch )
            {
                try
                {
                    assertSame( expectedBatch, batch );
                }
                finally
                {
                    latch.countDown();
                }
            }
        } )
        {
            control.steps( step );
            step.processors( threadCount );
            step.start( Step.ORDER_SEND_DOWNSTREAM );

            // WHEN
            step.receive( 0, expectedBatch );

            // THEN
            latch.await();
            control.assertHealthy();
        }
    }

    @Test
    public void shouldNotMissABeatUnderStress() throws Exception
    {
        // Idea is to have a constant load and then change number of processors randomly
        // there should be observed processing with various number of threads and they should
        // all fire as expected.

        // GIVEN
        SimpleStageControl control = new SimpleStageControl();
        int maxProcessorCount = 10;
        try ( Step<Object> step = new ForkedProcessorStep<Object>( control, "Stress", DEFAULT, maxProcessorCount )
        {
            private boolean[] seen = new boolean[maxProcessorCount];

            @Override
            protected void forkedProcess( int id, int processors, Object batch )
            {
                if ( seen[id] )
                {
                    fail( Arrays.toString( seen ) + " id:" + id + " processors:" + processors );
                }
                seen[id] = true;
            }

            @Override
            protected void process( Object batch, BatchSender sender ) throws Throwable
            {
                super.process( batch, sender );
                for ( int i = 0; i < forkedProcessors.size(); i++ )
                {
                    assertTrue( seen[i] );
                }
                Arrays.fill( seen, false );
            }
        } )
        {
            step.start( Step.ORDER_SEND_DOWNSTREAM );
            control.steps( step );
            t2.execute( ignore ->
            {
                while ( !step.isCompleted() )
                {
                    Thread.sleep( 10 );
                    step.processors( random.nextInt( maxProcessorCount ) + 1 );
                }
                return null;
            } );
            // Thread doing unpark on all the processor threads, just to verify that it can handle sprious wakeups
            t3.execute( ignore ->
            {
                while ( !step.isCompleted() )
                {
                    for ( Thread thread : Thread.getAllStackTraces().keySet() )
                    {
                        if ( thread.getName().contains( "Stress-" ) )
                        {
                            LockSupport.unpark( thread );
                        }
                    }
                }
                return null;
            } );

            // WHEN
            long endTime = currentTimeMillis() + SECONDS.toMillis( 1 );
            long count = 0;
            for ( ; currentTimeMillis() < endTime; count++ )
            {
                step.receive( count, new Object() );
            }
            step.endOfUpstream();
            while ( !step.isCompleted() )
            {
                Thread.sleep( 10 );
            }

            // THEN the proof is in the pudding, our forked processor has assertions of its own
            control.assertHealthy();
        }
    }
}
