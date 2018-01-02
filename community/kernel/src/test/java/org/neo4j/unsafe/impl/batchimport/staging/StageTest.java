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

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.unsafe.impl.batchimport.stats.Keys;

import static org.junit.Assert.assertEquals;

import static org.neo4j.unsafe.impl.batchimport.staging.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;

public class StageTest
{
    @Test
    public void shouldReceiveBatchesInOrder() throws Exception
    {
        // GIVEN
        Configuration config = new Configuration.Overridden( DEFAULT )
        {
            @Override
            public int batchSize()
            {
                return 10;
            }
        };
        Stage stage = new Stage( "Test stage", config, ORDER_SEND_DOWNSTREAM );
        long batches = 1000;
        final long items = batches*config.batchSize();
        stage.add( new ProducerStep( stage.control(), "Producer", config )
        {
            private final Object theObject = new Object();
            private long i;

            @Override
            protected Object nextBatchOrNull( long ticket, int batchSize )
            {
                if ( i >= items )
                {
                    return null;
                }

                Object[] batch = new Object[batchSize];
                Arrays.fill( batch, theObject );
                i += batchSize;
                return batch;
            }
        } );

        for ( int i = 0; i < 3; i++ )
        {
            stage.add( new ReceiveOrderAssertingStep( stage.control(), "Step" + i, config, i, false ) );
        }
        stage.add( new ReceiveOrderAssertingStep( stage.control(), "Final step", config, 0, true ) );

        // WHEN
        StageExecution execution = stage.execute();
        for ( Step<?> step : execution.steps() )
        {
            // we start off with two in each step
            step.incrementNumberOfProcessors();
        }
        new ExecutionSupervisor( ExecutionMonitors.invisible() ).supervise( execution );

        // THEN
        for ( Step<?> step : execution.steps() )
        {
            assertEquals( "For " + step, batches, step.stats().stat( Keys.done_batches ).asLong() );
        }
        stage.close();
    }

    private static class ReceiveOrderAssertingStep extends ProcessorStep<Object>
    {
        private final AtomicLong lastTicket = new AtomicLong();
        private final long processingTime;
        private final boolean endOfLine;

        ReceiveOrderAssertingStep( StageControl control, String name, Configuration config,
                long processingTime, boolean endOfLine )
        {
            super( control, name, config, 1 );
            this.processingTime = processingTime;
            this.endOfLine = endOfLine;
        }

        @Override
        public long receive( long ticket, Object batch )
        {
            assertEquals( "For " + batch + " in " + name(), lastTicket.getAndIncrement(), ticket );
            return super.receive( ticket, batch );
        }

        @Override
        protected void process( Object batch, BatchSender sender )
        {
            try
            {
                Thread.sleep( processingTime );
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }

            if ( !endOfLine )
            {
                sender.send( batch );
            }
        }
    }
}
