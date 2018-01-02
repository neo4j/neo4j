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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import org.neo4j.kernel.api.index.Reservation;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AggregatedReservationTest
{
    @Test
    public void shouldThrowWhenTooManyAggregatesAdded()
    {
        // Given
        int size = 5;
        AggregatedReservation aggregatedReservation = new AggregatedReservation( size );

        for ( int i = 0; i < size; i++ )
        {
            aggregatedReservation.add( mock( Reservation.class ) );
        }

        try
        {
            // When
            aggregatedReservation.add( mock( Reservation.class ) );
            fail( "Should have thrown " + IndexOutOfBoundsException.class.getSimpleName() );
        }
        catch ( IndexOutOfBoundsException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Too many aggregates" ) );
        }
    }

    @Test
    public void releaseShouldBeNullSafe()
    {
        // Given
        AggregatedReservation aggregatedReservation = new AggregatedReservation( 10 );

        Reservation aggregate1 = mock( Reservation.class );
        Reservation aggregate3 = mock( Reservation.class );

        aggregatedReservation.add( aggregate1 );
        aggregatedReservation.add( null );
        aggregatedReservation.add( aggregate3 );

        // When
        aggregatedReservation.release();

        // Then
        verify( aggregate1 ).release();
        verify( aggregate3 ).release();
    }

    @Test
    public void shouldReleaseAllAggregatedReservations()
    {
        // Given
        AggregatedReservation aggregatedReservation = new AggregatedReservation( 3 );

        Reservation aggregate1 = mock( Reservation.class );
        Reservation aggregate2 = mock( Reservation.class );
        Reservation aggregate3 = mock( Reservation.class );

        aggregatedReservation.add( aggregate1 );
        aggregatedReservation.add( aggregate2 );
        aggregatedReservation.add( aggregate3 );

        // When
        aggregatedReservation.release();

        // Then
        verify( aggregate1 ).release();
        verify( aggregate2 ).release();
        verify( aggregate3 ).release();
    }

    @Test
    public void shouldReleaseAllAggregatedReservationsEvenIfOneOfThemThrows()
    {
        // Given
        AggregatedReservation aggregatedReservation = new AggregatedReservation( 3 );

        Reservation aggregate1 = mock( Reservation.class );
        Reservation aggregate2 = mock( Reservation.class );
        IllegalStateException exception = new IllegalStateException();
        doThrow( exception ).when( aggregate2 ).release();
        Reservation aggregate3 = mock( Reservation.class );

        aggregatedReservation.add( aggregate1 );
        aggregatedReservation.add( aggregate2 );
        aggregatedReservation.add( aggregate3 );

        try
        {
            // When
            aggregatedReservation.release();
            fail( "Should have thrown " + IllegalStateException.class.getSimpleName() );
        }
        catch ( IllegalStateException e )
        {
            assertSame( exception, e );
        }

        // Then
        verify( aggregate1 ).release();
        verify( aggregate2 ).release();
        verify( aggregate3 ).release();
    }

    @Test
    public void shouldThrowLaunderedException()
    {
        // Given
        AggregatedReservation aggregatedReservation = new AggregatedReservation( 1 );

        Reservation reservation = mock( Reservation.class );
        RuntimeException exception = new RuntimeException( "Error" );
        doThrow( exception ).when( reservation ).release();
        aggregatedReservation.add( reservation );

        try
        {
            // When
            aggregatedReservation.release();
            fail( "Should have thrown " + RuntimeException.class.getSimpleName() );
        }
        catch ( RuntimeException e )
        {
            // Then
            assertSame( e, exception );
            assertNull( e.getCause() );
        }
    }

    @Test
    public void shouldNotAllowRecursiveAggregations()
    {
        // Given
        AggregatedReservation aggregatedReservation = new AggregatedReservation( 1 );

        try
        {
            // When
            aggregatedReservation.add( aggregatedReservation );
            fail( "Should have thrown " + IllegalArgumentException.class.getSimpleName() );
        }
        catch ( IllegalArgumentException e )
        {
            // Then
            assertThat( e.getMessage(), startsWith( "Recursive" ) );
        }
    }
}
