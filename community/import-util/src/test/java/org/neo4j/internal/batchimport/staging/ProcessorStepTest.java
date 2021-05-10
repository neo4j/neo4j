/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.batchimport.staging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.rule.OtherThreadRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.batchimport.executor.ProcessorScheduler.SPAWN_THREAD;
import static org.neo4j.internal.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class ProcessorStepTest
{
    private final OtherThreadRule t2 = new OtherThreadRule();

    @BeforeEach
    void setUp()
    {
        t2.init("processor-step");
    }

    @AfterEach
    void tearDown()
    {
        t2.close();
    }

    @Test
    void shouldUpholdProcessOrderingGuarantee() throws Exception
    {
        // GIVEN
        StageControl control = new SimpleStageControl();
        try ( MyProcessorStep step = new MyProcessorStep( control, 0 ) )
        {
            step.start( ORDER_SEND_DOWNSTREAM );
            step.processors( 4 ); // now at 5

            // WHEN
            int batches = 10;
            for ( int i = 0; i < batches; i++ )
            {
                step.receive( i, i );
            }
            step.endOfUpstream();
            step.awaitCompleted();

            // THEN
            assertEquals( batches, step.nextExpected.get() );
        }
    }

    @Test
    void tracePageCacheAccessOnProcess() throws Exception
    {
        StageControl control = new SimpleStageControl();
        var cacheTracer = new DefaultPageCacheTracer();
        int batches = 10;
        try ( MyProcessorStep step = new MyProcessorStep( control, 0, cacheTracer ) )
        {
            step.start( Step.ORDER_SEND_DOWNSTREAM );

            for ( int i = 0; i < batches; i++ )
            {
                step.receive( i, i );
            }
            step.endOfUpstream();
            step.awaitCompleted();

            assertEquals( batches, step.nextExpected.get() );
        }

        assertThat( cacheTracer.pins() ).isEqualTo( batches );
        assertThat( cacheTracer.unpins() ).isEqualTo( batches );
    }

    @Test
    void shouldHaveTaskQueueSizeEqualToMaxNumberOfProcessors() throws Exception
    {
        // GIVEN
        StageControl control = new SimpleStageControl();
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
        Future<Void> receiveFuture;
        try ( ProcessorStep<Void> step = new BlockingProcessorStep<>( control, configuration, processors, latch ) )
        {
            step.start( ORDER_SEND_DOWNSTREAM );
            step.processors( 1 ); // now at 2
            // adding up to max processors should be fine
            for ( int i = 0; i < processors + maxProcessors /* +1 since we allow queueing one more*/; i++ )
            {
                step.receive( i, null );
            }

            // WHEN
            receiveFuture = t2.execute( receive( processors, step ) );
            t2.get().waitUntilThreadState( Thread.State.TIMED_WAITING );
            latch.countDown();

            // THEN
            receiveFuture.get();
        }
    }

    @Test
    void shouldRecycleDoneBatches() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        when( control.scheduler() ).thenReturn( SPAWN_THREAD );
        try ( MyProcessorStep step = new MyProcessorStep( control, 0 ) )
        {
            step.start( ORDER_SEND_DOWNSTREAM );

            // WHEN
            int batches = 10;
            for ( int i = 0; i < batches; i++ )
            {
                step.receive( i, i );
            }
            step.endOfUpstream();
            step.awaitCompleted();

            // THEN
            verify( control, times( batches ) ).recycle( any() );
        }
    }

    @Test
    public void shouldBeAbleToPropagatePanicOnBlockedProcessorsWhenLast() throws InterruptedException
    {
        shouldBeAbleToPropagatePanicOnBlockedProcessors( 2, 1 );
    }

    @Test
    public void shouldBeAbleToPropagatePanicOnBlockedProcessorsWhenNotLast() throws InterruptedException
    {
        shouldBeAbleToPropagatePanicOnBlockedProcessors( 3, 1 );
    }

    private void shouldBeAbleToPropagatePanicOnBlockedProcessors( int numProcessors, int failingProcessorIndex ) throws InterruptedException
    {
        // Given
        String exceptionMessage = "Failing just for fun";
        Configuration configuration = Configuration.DEFAULT;
        CountDownLatch latch = new CountDownLatch( 1 );
        TrackingPanicMonitor panicMonitor = new TrackingPanicMonitor();
        Stage stage = new Stage( "Test", "Part", configuration, ORDER_SEND_DOWNSTREAM, SPAWN_THREAD, panicMonitor );
        stage.add( intProducer( configuration, stage, configuration.maxNumberOfProcessors() * 2 ) );
        ProcessorStep<Integer> failingProcessor = null;
        for ( int i = 0; i < numProcessors; i++ )
        {
            if ( failingProcessorIndex == i )
            {
                failingProcessor = new BlockingProcessorStep<>( stage.control(), configuration, 1, latch )
                {
                    @Override
                    protected void process( Integer batch, BatchSender sender, CursorContext cursorContext ) throws Throwable
                    {
                        // Block until the latch is released below
                        super.process( batch, sender, cursorContext );
                        // Then immediately throw exception so that a panic will be issued
                        throw new RuntimeException( exceptionMessage );
                    }
                };
                stage.add( failingProcessor );
            }
            else
            {
                stage.add( intProcessor( configuration, stage ) );
            }
        }

        try
        {
            // When
            StageExecution execution = stage.execute();
            while ( failingProcessor.stats().stat( Keys.received_batches ).asLong() < configuration.maxNumberOfProcessors() + 1 )
            {
                Thread.sleep( 10 );
            }
            latch.countDown();

            // Then
            execution.awaitCompletion();
            RuntimeException exception = assertThrows( RuntimeException.class, execution::assertHealthy );
            assertEquals( exceptionMessage, exception.getMessage() );
        }
        finally
        {
            stage.close();
        }
        assertTrue( panicMonitor.hasReceivedPanic() );
    }

    private static ProducerStep intProducer( Configuration configuration, Stage stage, int batches )
    {
        return new ProducerStep( stage.control(), configuration )
        {
            @Override
            protected void process()
            {
                for ( int i = 0; i < batches; i++ )
                {
                    sendDownstream( i );
                }
            }

            @Override
            protected long position()
            {
                return 0;
            }
        };
    }

    private static ProcessorStep<Integer> intProcessor( Configuration configuration, Stage stage )
    {
        return new ProcessorStep<>( stage.control(), "processor", configuration, 1, NULL )
        {
            @Override
            protected void process( Integer batch, BatchSender sender, CursorContext cursorContext )
            {
                sender.send( batch );
            }
        };
    }

    private static class BlockingProcessorStep<T> extends ProcessorStep<T>
    {
        private final CountDownLatch latch;

        BlockingProcessorStep( StageControl control, Configuration configuration,
                int maxProcessors, CountDownLatch latch )
        {
            super( control, "test", configuration, maxProcessors, NULL );
            this.latch = latch;
        }

        @Override
        protected void process( T batch, BatchSender sender, CursorContext cursorContext ) throws Throwable
        {
            latch.await();
        }
    }

    private static class MyProcessorStep extends ProcessorStep<Integer>
    {
        private final AtomicInteger nextExpected = new AtomicInteger();

        private MyProcessorStep( StageControl control, int maxProcessors )
        {
            this( control, maxProcessors, NULL );
        }

        private MyProcessorStep( StageControl control, int maxProcessors, PageCacheTracer pageCacheTracer )
        {
            super( control, "test", Configuration.DEFAULT, maxProcessors, pageCacheTracer );
        }

        @Override
        protected void process( Integer batch, BatchSender sender, CursorContext cursorContext )
        {
            var pinEvent = cursorContext.getCursorTracer().beginPin( false, 1, null );
            pinEvent.hit();
            pinEvent.done();
            nextExpected.incrementAndGet();
        }
    }

    private static Callable<Void> receive( final int processors, final ProcessorStep<Void> step )
    {
        return () ->
        {
            step.receive( processors, null );
            return null;
        };
    }
}
