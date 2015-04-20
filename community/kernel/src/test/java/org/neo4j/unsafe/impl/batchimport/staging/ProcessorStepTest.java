/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Resource;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_PROCESS;

public class ProcessorStepTest
{
    @Test
    public void shouldUpholdProcessOrderingGuarantee() throws Exception
    {
        // GIVEN
        StageControl control = mock( StageControl.class );
        MyProcessorStep step = new MyProcessorStep( control );
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

    public class MyProcessorStep extends ProcessorStep<Integer>
    {
        private final AtomicInteger nextExpected = new AtomicInteger();

        private MyProcessorStep( StageControl control )
        {
            super( control, "test", Configuration.DEFAULT, 0 );
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
}
