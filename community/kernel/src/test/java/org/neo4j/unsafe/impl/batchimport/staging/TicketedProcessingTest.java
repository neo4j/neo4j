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

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.unsafe.impl.batchimport.executor.ParkStrategy;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutionPanicException;

import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.test.DoubleLatch.awaitLatch;

public class TicketedProcessingTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldReturnTicketsInOrder() throws Exception
    {
        // GIVEN
        int items = 1_000;
        ParkStrategy park = new ParkStrategy.Park( 2, MILLISECONDS );
        BiFunction<Integer,Void,Integer> processor = ( from, ignore ) ->
        {
            if ( ThreadLocalRandom.current().nextFloat() < 0.01f )
            {
                park.park( Thread.currentThread() );
            }
            return from * 2;
        };
        int processorCount = Runtime.getRuntime().availableProcessors();
        Future<Void> assertions;
        try ( TicketedProcessing<Integer,Void,Integer> processing = new TicketedProcessing<>(
                "Doubler", processorCount, processor, () -> null ) )
        {
            processing.processors( processorCount - processing.processors( 0 ) );

            // WHEN
            assertions = t2.execute( state ->
            {
                for ( int i = 0; i < items; i++ )
                {
                    Integer next = processing.next();
                    assertNotNull( next );
                    assertEquals( i * 2, next.intValue() );
                }
                assertNull( processing.next() );
                return null;
            } );
            for ( int i = 0; i < items; i++ )
            {
                processing.submit( i );
            }
        }

        // THEN
        assertions.get();
    }

    @Test
    public void shouldNotBeAbleToSubmitTooFarAhead() throws Exception
    {
        // GIVEN
        try ( TicketedProcessing<StringJob,Void,Integer> processing = new TicketedProcessing<>( "Parser", 2,
                ( job, state ) ->
                {
                    awaitLatch( job.latch );
                    return parseInt( job.string );
                }, () -> null ) )
        {
            processing.processors( 1 ); // now at 2
            StringJob firstJob = new StringJob( "1" );
            processing.submit( firstJob );
            StringJob secondJob = new StringJob( "2" );
            processing.submit( secondJob );
            // now both processors have taken the 2 submitted jobs and so there should be 2 slots available

            // WHEN
            StringJob thirdJob = new StringJob( "3" );
            thirdJob.latch.countDown();
            processing.submit( thirdJob );
            StringJob fourthJob = new StringJob( "4" );
            fourthJob.latch.countDown();
            processing.submit( fourthJob );

            StringJob fifthJob = new StringJob( "5" );
            fifthJob.latch.countDown();
            Future<Void> fifthSubmit = t2.execute( state ->
            {
                processing.submit( fifthJob );
                return null;
            } );
            t2.get().waitUntilThreadState( Thread.State.TIMED_WAITING, Thread.State.WAITING );
            firstJob.latch.countDown();
            assertEquals( 1, processing.next().intValue() );
            fifthSubmit.get();
            secondJob.latch.countDown();
            assertEquals( 2, processing.next().intValue() );
            assertEquals( 3, processing.next().intValue() );
        }
    }

    @Test
    public void shouldNoticeSlurpPanic() throws Exception
    {
        // GIVEN
        IllegalStateException failure = new IllegalStateException( "Consistently failing" );
        try ( TicketedProcessing<StringJob,Void,Integer> processing = new TicketedProcessing<>( "Parser", 2,
                ( job, state ) -> parseInt( job.string ), () -> null ) )
        {
            processing.processors( 1 );

            // WHEN
            Future<Void> slurp = processing.slurp( failingIterator( iterator(
                    () -> new StringJob( "1" ),
                    () ->
                    {
                        throw failure;
                    } ) ), true );
            try
            {
                while ( processing.next() != null )
                {
                    // Just read through them
                }
                fail( "Should have noticed the slurp failure" );
            }
            catch ( TaskExecutionPanicException e )
            {
                // THEN good
                assertSame( failure, e.getCause() );
            }

            // also THEN
            try
            {
                slurp.get();
                fail( "Should have noticed the slurp failure" );
            }
            catch ( ExecutionException e )
            {
                assertSame( failure, e.getCause() );
            }
        }
    }

    private static Iterator<StringJob> failingIterator( Iterator<Supplier<StringJob>> suppliers )
    {
        return new IteratorWrapper<StringJob,Supplier<StringJob>>( suppliers )
        {
            @Override
            protected StringJob underlyingObjectToObject( Supplier<StringJob> object )
            {
                return object.get();
            }
        };
    }

    private static class StringJob
    {
        final String string;
        final CountDownLatch latch = new CountDownLatch( 1 );

        StringJob( String string )
        {
            this.string = string;
        }
    }
}
