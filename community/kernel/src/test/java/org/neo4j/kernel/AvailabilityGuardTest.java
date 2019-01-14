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
package org.neo4j.kernel;

import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.AvailabilityGuard.UnavailableException;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.time.Clocks;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.AvailabilityGuard.availabilityRequirement;
import static org.neo4j.logging.NullLog.getInstance;

public class AvailabilityGuardTest
{
    private static final AvailabilityGuard.AvailabilityRequirement REQUIREMENT_1 = availabilityRequirement( "Requirement 1" );
    private static final AvailabilityGuard.AvailabilityRequirement REQUIREMENT_2 = availabilityRequirement( "Requirement 2" );

    private Clock clock = Clocks.systemClock();

    @Test
    public void logOnAvailabilityChange()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );

        // When starting out
        verifyZeroInteractions( log );

        // When requirement is added
        availabilityGuard.require( REQUIREMENT_1 );

        // Then log should have been called
        verify( log, atLeastOnce() ).info( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should have been called
        verify( log, atLeast( 2 ) ).info( anyString() );

        // When requirement is added
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // Then log should have been called
        verify( log, atLeast( 3 ) ).info( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should not have been called
        verify( log, atMost( 3 ) ).info( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_2 );

        // Then log should have been called
        verify( log, atLeast( 4 ) ).info( anyString() );
        verify( log, atMost( 4 ) ).info( anyString() );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenAwaitThenTimeoutAndReturnFalse()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        boolean result = availabilityGuard.isAvailable( 1000 );

        // Then
        assertThat( result, equalTo( false ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenAwaitThenActuallyWaitGivenTimeout()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        long timeout = 1000;
        long start = clock.millis();
        boolean result = availabilityGuard.isAvailable( timeout );
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( false ) );
        assertThat( waitTime, greaterThanOrEqualTo( timeout ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitThenTimeoutAndReturnFalse()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        long start = clock.millis();
        long timeout = 1000;
        availabilityGuard.fulfill( REQUIREMENT_1 );
        boolean result = availabilityGuard.isAvailable( timeout );
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertFalse( result );
        assertThat( waitTime, greaterThanOrEqualTo( timeout ) );

    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantEachAndAwaitThenTrue()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.fulfill( REQUIREMENT_2 );

        assertTrue( availabilityGuard.isAvailable( 1000 ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantTwiceAndDenyOnceAndAwaitThenTimeoutAndReturnFalse()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        long start = clock.millis();
        long timeout = 1000;
        boolean result = availabilityGuard.isAvailable( timeout );
        long end = clock.millis();

        // Then
        long waitTime = end - start;
        assertFalse( result );
        assertThat( waitTime, greaterThanOrEqualTo( timeout ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitAndGrantAgainThenReturnTrue()
    {
        // Given
        Log log = mock( Log.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        availabilityGuard.fulfill( REQUIREMENT_2 );
        assertFalse( availabilityGuard.isAvailable( 100 ) );

        availabilityGuard.fulfill( REQUIREMENT_1 );
        assertTrue( availabilityGuard.isAvailable( 100 ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenGrantThenNotifyListeners()
    {
        // Given
        Log log = mock( Log.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityGuard.AvailabilityListener availabilityListener = new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
                notified.set( true );
            }

            @Override
            public void unavailable()
            {
            }
        };

        availabilityGuard.addListener( availabilityListener );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );

        // Then
        assertThat( notified.get(), equalTo( true ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenGrantAndDenyThenNotifyListeners()
    {
        // Given
        Log log = mock( Log.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityGuard.AvailabilityListener availabilityListener = new AvailabilityGuard.AvailabilityListener()
        {
            @Override
            public void available()
            {
            }

            @Override
            public void unavailable()
            {
                notified.set( true );
            }
        };

        availabilityGuard.addListener( availabilityListener );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_1 );

        // Then
        assertThat( notified.get(), equalTo( true ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenShutdownThenInstantlyDenyAccess()
    {
        // Given
        Clock clock = Mockito.mock( Clock.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, NullLog.getInstance() );
        availabilityGuard.require( REQUIREMENT_1 );

        // When
        availabilityGuard.shutdown();

        // Then
        assertFalse( availabilityGuard.isAvailable( 1000 ) );
        verifyZeroInteractions( clock );
    }

    @Test
    public void shouldExplainWhoIsBlockingAccess()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );

        // When
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // Then
        assertThat( availabilityGuard.describeWhoIsBlocking(), equalTo( "2 reasons for blocking: Requirement 1, Requirement 2." ) );
    }

    @Test
    public void shouldExplainBlockersOnCheckAvailable() throws Exception
    {
        // GIVEN
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clocks.systemClock(), getInstance() );
        // At this point it should be available
        availabilityGuard.checkAvailable();

        // WHEN
        availabilityGuard.require( REQUIREMENT_1 );

        // THEN
        try
        {
            availabilityGuard.checkAvailable();
            fail( "Should not be available" );
        }
        catch ( UnavailableException e )
        {
            assertThat( e.getMessage(), containsString( REQUIREMENT_1.description() ) );
        }
    }
}
