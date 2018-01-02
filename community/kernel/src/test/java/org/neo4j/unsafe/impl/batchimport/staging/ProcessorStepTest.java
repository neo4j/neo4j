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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Resource;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_PROCESS;

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
        step.start( ORDER_PROCESS );
        while ( step.numberOfProcessors() < 5 )
        {
            step.incrementNumberOfProcessors();
        }

        // WHEN
        int batches = 10;
        for ( int i = 0; i < batches; i++ )
        {
            step.receive( i, i );
        }
        step.endOfUpstream();
        while ( !step.isCompleted() )
        {
            verifyNoMoreInteractions( control );
        }

        // THEN
        assertEquals( batches, step.nextExpected.get() );
        step.close();
    }

    @Test
    public void shouldHaveTaskQueueSizeEqualToNumberOfProcessorsIfSpecificallySet() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        final CountDownLatch latch = new CountDownLatch( 1 );
        final int processors = 2;
        final ProcessorStep<Void> step = new BlockingProcessorStep( control, processors, latch );
        step.start( ORDER_PROCESS );
        step.incrementNumberOfProcessors(); // now at 2
        // adding two should be fine
        for ( int i = 0; i < processors+1 /* +1 since we allow queueing one more*/; i++ )
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
    public void shouldHaveTaskQueueSizeEqualToCurrentNumberOfProcessorsIfNotSpecificallySet() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        final CountDownLatch latch = new CountDownLatch( 1 );
        final ProcessorStep<Void> step = new BlockingProcessorStep( control, 0, latch );
        step.start( ORDER_PROCESS );
        step.incrementNumberOfProcessors(); // now at 2
        step.incrementNumberOfProcessors(); // now at 3
        // adding two should be fine
        for ( int i = 0; i < step.numberOfProcessors()+1 /* +1 since we allow queueing one more*/; i++ )
        {
            step.receive( i, null );
        }

        // WHEN
        Future<Void> receiveFuture = t2.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                step.receive( step.numberOfProcessors(), null );
                return null;
            }
        } );
        t2.get().waitUntilThreadState( Thread.State.TIMED_WAITING );
        latch.countDown();

        // THEN
        receiveFuture.get();
    }

    private static class BlockingProcessorStep extends ProcessorStep<Void>
    {
        private final CountDownLatch latch;

        public BlockingProcessorStep( StageControl control, int maxProcessors, CountDownLatch latch )
        {
            super( control, "test", Configuration.DEFAULT, maxProcessors );
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
        protected Resource permit( Integer batch ) throws Throwable
        {
            // Sleep a little to allow other processors much more easily to catch up and have
            // a chance to race, if permit ordering guarantee isn't upheld, that is.
            Thread.sleep( 10 );
            assertEquals( nextExpected.getAndIncrement(), batch.intValue() );
            return super.permit( batch );
        }

        @Override
        protected void process( Integer batch, BatchSender sender ) throws Throwable
        {   // No processing in this test
        }
    }

    private WorkerCommand<Void,Void> receive( final int processors, final ProcessorStep<Void> step )
    {
        return new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                step.receive( processors, null );
                return null;
            }
        };
    }
}
