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
package org.neo4j.kernel;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.helpers.TickingClock;
import org.neo4j.logging.Log;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.kernel.AvailabilityGuard.availabilityRequirement;

public class AvailabilityGuardTest
{
    private static final AvailabilityGuard.AvailabilityRequirement REQUIREMENT_1 = availabilityRequirement( "Requirement 1" );
    private static final AvailabilityGuard.AvailabilityRequirement REQUIREMENT_2 = availabilityRequirement( "Requirement 2" );

    @Test
    public void logOnAvailabilityChange() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );

        // When starting out
        verifyZeroInteractions( log );

        // When requirement is added
        availabilityGuard.require( REQUIREMENT_1 );

        // Then log should have been called
        verify( log, atLeastOnce() ).debug( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should have been called
        verify( log, atLeast( 2 ) ).debug( anyString() );

        // When requirement is added
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // Then log should have been called
        verify( log, atLeast( 3 ) ).debug( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should not have been called
        verify( log, atMost( 3 ) ).debug( anyString() );

        // When requirement fulfilled
        availabilityGuard.fulfill( REQUIREMENT_2 );

        // Then log should have been called
        verify( log, atLeast( 4 ) ).debug( anyString() );
        verify( log, atMost( 4 ) ).debug( anyString() );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenAwaitThenTimeoutAndReturnFalse() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
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
    public void givenAccessGuardWith2ConditionsWhenAwaitThenActuallyWaitGivenTimeout() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        long start = clock.currentTimeMillis();
        boolean result = availabilityGuard.isAvailable( 1000 );
        long end = clock.currentTimeMillis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( false ) );
        assertThat( waitTime, equalTo( 1200L ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitThenTimeoutAndReturnFalse() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        long start = clock.currentTimeMillis();
        availabilityGuard.fulfill( REQUIREMENT_1 );
        boolean result = availabilityGuard.isAvailable( 1000 );
        long end = clock.currentTimeMillis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( false ) );
        assertThat( waitTime, equalTo( 1200L ) );

    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantEachAndAwaitThenTrue() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.fulfill( REQUIREMENT_2 );

        long start = clock.currentTimeMillis();
        boolean result = availabilityGuard.isAvailable( 1000 );
        long end = clock.currentTimeMillis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( true ) );
        assertThat( waitTime, equalTo( 100L ) );

    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantTwiceAndDenyOnceAndAwaitThenTimeoutAndReturnFalse() throws
            Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.fulfill( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        long start = clock.currentTimeMillis();
        boolean result = availabilityGuard.isAvailable( 1000 );
        long end = clock.currentTimeMillis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( false ) );
        assertThat( waitTime, equalTo( 1200L ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantOnceAndAwaitAndGrantAgainDuringAwaitThenReturnTrue() throws
            Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // When
        clock.at( 500, new Runnable()
        {
            @Override
            public void run()
            {
                availabilityGuard.fulfill( REQUIREMENT_1 );
            }
        } );
        availabilityGuard.fulfill( REQUIREMENT_2 );

        long start = clock.currentTimeMillis();
        boolean result = availabilityGuard.isAvailable( 1000 );
        long end = clock.currentTimeMillis();

        // Then
        long waitTime = end - start;
        assertThat( result, equalTo( true ) );
        assertThat( waitTime, equalTo( 600L ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenGrantThenNotifyListeners() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
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
    public void givenAccessGuardWithConditionWhenGrantAndDenyThenNotifyListeners() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
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
    public void givenAccessGuardWithConditionWhenShutdownThenInstantlyDenyAccess() throws Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        final AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );
        availabilityGuard.require( REQUIREMENT_1 );

        // When
        availabilityGuard.shutdown();

        // Then
        boolean result = availabilityGuard.isAvailable( 1000 );

        assertThat( result, equalTo( false ) );
        assertThat( clock.currentTimeMillis(), equalTo( 0L ) );
    }

    @Test
    public void shouldExplainWhoIsBlockingAccess() throws
            Exception
    {
        // Given
        TickingClock clock = new TickingClock( 0, 100 );
        Log log = mock( Log.class );
        AvailabilityGuard availabilityGuard = new AvailabilityGuard( clock, log );

        // When
        availabilityGuard.require( REQUIREMENT_1 );
        availabilityGuard.require( REQUIREMENT_2 );

        // Then
        assertThat( availabilityGuard.describeWhoIsBlocking(), equalTo( "2 reasons for blocking: Requirement 1, Requirement 2." ) );
    }
}
