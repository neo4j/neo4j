/**
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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static org.junit.Assert.assertEquals;

public class StageTest
{
    @Test
    public void shouldReceiveBatchesInOrder() throws Exception
    {
        // GIVEN
        Configuration config = new Configuration.Default();
        Stage stage = new Stage( new DevNullLoggingService(), "Test stage", config );
        int batchSize = 10;
        long batches = 1000;
        final long items = batches*batchSize;
        stage.add( new ProducerStep<Object>( stage.control(), "Producer", batchSize )
        {
            private long i = 0;
            private final Object theObject = new Object();

            @Override
            protected Object nextOrNull()
            {
                return ++i > items ? null : theObject;
            }
        } );

        for ( int i = 0; i < 3; i++ )
        {
            stage.add( new ReceiveOrderAssertingStep( stage.control(), "Step" + i, 20, 2, i ) );
        }

        stage.add( new LastReceiveOrderAssertingStep( stage.control(), "Final step", 20, 2, 0 ) );

        // WHEN
        StageExecution execution = stage.execute();
        new SilentExecutionMonitor().monitor( execution );

        // THEN
        for ( StepStats stats : execution.stats() )
        {
            assertEquals( batches, stats.stat( Keys.done_batches ).asLong() );
        }
    }

    private static class ReceiveOrderAssertingStep extends ExecutorServiceStep<Object>
    {
        private final AtomicLong lastTicket = new AtomicLong();
        private final long processingTime;

        ReceiveOrderAssertingStep( StageControl control, String name, int workAheadSize, int numberOfExecutors,
                long processingTime )
        {
            super( control, name, workAheadSize, numberOfExecutors );
            this.processingTime = processingTime;
        }

        @Override
        public long receive( long ticket, Object batch )
        {
            assertEquals( lastTicket.incrementAndGet(), ticket );
            return super.receive( ticket, batch );
        }

        @Override
        protected Object process( long ticket, Object batch )
        {
            try
            {
                Thread.sleep( processingTime );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
            return batch;
        }
    }

    private static class LastReceiveOrderAssertingStep extends ReceiveOrderAssertingStep
    {
        LastReceiveOrderAssertingStep( StageControl control, String name, int workAheadSize,
                int numberOfExecutors, long processingTime )
        {
            super( control, name, workAheadSize, numberOfExecutors, processingTime );
        }

        @Override
        protected Object process( long ticket, Object batch )
        {
            super.process( ticket, batch );
            return null;
        }
    }
}
