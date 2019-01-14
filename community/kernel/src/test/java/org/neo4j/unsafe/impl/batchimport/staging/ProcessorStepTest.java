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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

public class ProcessorStepTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldUpholdProcessOrderingGuarantee() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        MyProcessorStep step = new MyProcessorStep( control, 0 );
        step.start( ORDER_SEND_DOWNSTREAM );
        step.processors( 4 ); // now at 5

        // WHEN
        int batches = 10;
        for ( int i = 0; i < batches; i++ )
        {
            step.receive( i, i );
        }
        step.endOfUpstream();
        while ( !step.isCompleted() )
        {
            Thread.sleep( 10 );
        }

        // THEN
        assertEquals( batches, step.nextExpected.get() );
        step.close();
    }

    @Test
    public void shouldHaveTaskQueueSizeEqualToMaxNumberOfProcessors() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        final CountDownLatch latch = new CountDownLatch( 1 );
        final int processors = 2;
        int maxProcessors = 5;
        Configuration configuration = new Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return maxProcessors;
            }
        };
        final ProcessorStep<Void> step = new BlockingProcessorStep( control, configuration, processors, latch );
        step.start( ORDER_SEND_DOWNSTREAM );
        step.processors( 1 ); // now at 2
        // adding up to max processors should be fine
        for ( int i = 0; i < processors + maxProcessors /* +1 since we allow queueing one more*/; i++ )
        {
            step.receive( i, null );
        }

        // WHEN
        Future<Void> receiveFuture = t2.execute( receive( processors, step ) );
        t2.get().waitUntilThreadState( Thread.State.TIMED_WAITING );
        latch.countDown();

        // THEN
        receiveFuture.get();
    }

    @Test
    public void shouldRecycleDoneBatches() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        MyProcessorStep step = new MyProcessorStep( control, 0 );
        step.start( ORDER_SEND_DOWNSTREAM );

        // WHEN
        int batches = 10;
        for ( int i = 0; i < batches; i++ )
        {
            step.receive( i, i );
        }
        step.endOfUpstream();
        while ( !step.isCompleted() )
        {
            Thread.sleep( 10 );
        }

        // THEN
        verify( control, times( batches ) ).recycle( any() );
        step.close();
    }

    private static class BlockingProcessorStep extends ProcessorStep<Void>
    {
        private final CountDownLatch latch;

        BlockingProcessorStep( StageControl control, Configuration configuration,
                int maxProcessors, CountDownLatch latch )
        {
            super( control, "test", configuration, maxProcessors );
            this.latch = latch;
        }

        @Override
        protected void process( Void batch, BatchSender sender ) throws Throwable
        {
            latch.await();
        }
    }

    private static class MyProcessorStep extends ProcessorStep<Integer>
    {
        private final AtomicInteger nextExpected = new AtomicInteger();

        private MyProcessorStep( StageControl control, int maxProcessors )
        {
            super( control, "test", Configuration.DEFAULT, maxProcessors );
        }

        @Override
        protected void process( Integer batch, BatchSender sender )
        {   // No processing in this test
            nextExpected.incrementAndGet();
        }
    }

    private WorkerCommand<Void,Void> receive( final int processors, final ProcessorStep<Void> step )
    {
        return state ->
        {
            step.receive( processors, null );
            return null;
        };
    }
}
