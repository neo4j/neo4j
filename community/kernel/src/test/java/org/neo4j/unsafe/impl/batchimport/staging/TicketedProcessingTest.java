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

import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.unsafe.impl.batchimport.executor.ParkStrategy;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TicketedProcessingTest
{
    @Rule
    public final OtherThreadRule<Void> asserter = new OtherThreadRule<>();

    @Test
    public void shouldReturnTicketsInOrder() throws Exception
    {
        // GIVEN
        int items = 1_000;
        ParkStrategy park = new ParkStrategy.Park( 2, MILLISECONDS );
        BiFunction<Integer,Void,Integer> processor = (from,ignore) ->
        {
            if ( ThreadLocalRandom.current().nextFloat() < 0.01f )
            {
                park.park( Thread.currentThread() );
            }
            return from*2;
        };
        int processorCount = Runtime.getRuntime().availableProcessors();
        TicketedProcessing<Integer,Void,Integer> processing = new TicketedProcessing<>(
                "Doubler", processorCount, processor, () -> null );
        processing.setNumberOfProcessors( processorCount );

        // WHEN
        Future<Void> assertions = asserter.execute( new WorkerCommand<Void,Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                for ( int i = 0; i < items; i++ )
                {
                    Integer next = processing.next();
                    assertNotNull( next );
                    assertEquals( i*2, next.intValue() );
                }
                assertNull( processing.next() );
                return null;
            }
        } );
        for ( int i = 0; i < items; i++ )
        {
            processing.submit( i, i );
        }
        processing.shutdown( true );

        // THEN
        assertions.get();
    }
}
