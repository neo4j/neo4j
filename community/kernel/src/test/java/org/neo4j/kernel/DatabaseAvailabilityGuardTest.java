/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.mockito.verification.VerificationMode;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.availability.AvailabilityRequirement;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.DescriptiveAvailabilityRequirement;
import org.neo4j.kernel.availability.UnavailableException;
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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.NullLog.getInstance;

public class DatabaseAvailabilityGuardTest
{
    private static final AvailabilityRequirement REQUIREMENT_1 = new DescriptiveAvailabilityRequirement( "Requirement 1" );
    private static final AvailabilityRequirement REQUIREMENT_2 = new DescriptiveAvailabilityRequirement( "Requirement 2" );

    private final Clock clock = Clocks.systemClock();

    @Test
    public void logOnAvailabilityChange()
    {
        // Given
        Log log = mock( Log.class );
        AvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );

        // When starting out
        verifyZeroInteractions( log );

        // When requirement is added
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        // Then log should have been called
        verifyLogging( log, atLeastOnce() );

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should have been called
        verifyLogging( log, times( 4 ) );

        // When requirement is added
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // Then log should have been called
        verifyLogging( log, times( 6 ) );

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );

        // Then log should not have been called
        verifyLogging( log, times( 6 ) );

        // When requirement fulfilled
        databaseAvailabilityGuard.fulfill( REQUIREMENT_2 );

        // Then log should have been called
        verifyLogging( log, times( 8 ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenAwaitThenTimeoutAndReturnFalse()
    {
        // Given
        Log log = mock( Log.class );
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // When
        boolean result = databaseAvailabilityGuard.isAvailable( 1000 );

        // Then
        assertThat( result, equalTo( false ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenAwaitThenActuallyWaitGivenTimeout()
    {
        // Given
        Log log = mock( Log.class );
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // When
        long timeout = 1000;
        long start = clock.millis();
        boolean result = databaseAvailabilityGuard.isAvailable( timeout );
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
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // When
        long start = clock.millis();
        long timeout = 1000;
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        boolean result = databaseAvailabilityGuard.isAvailable( timeout );
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
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // When
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        databaseAvailabilityGuard.fulfill( REQUIREMENT_2 );

        assertTrue( databaseAvailabilityGuard.isAvailable( 1000 ) );
    }

    @Test
    public void givenAccessGuardWith2ConditionsWhenGrantTwiceAndDenyOnceAndAwaitThenTimeoutAndReturnFalse()
    {
        // Given
        Log log = mock( Log.class );
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // When
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        long start = clock.millis();
        long timeout = 1000;
        boolean result = databaseAvailabilityGuard.isAvailable( timeout );
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
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        databaseAvailabilityGuard.fulfill( REQUIREMENT_2 );
        assertFalse( databaseAvailabilityGuard.isAvailable( 100 ) );

        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        assertTrue( databaseAvailabilityGuard.isAvailable( 100 ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenGrantThenNotifyListeners()
    {
        // Given
        Log log = mock( Log.class );
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityListener availabilityListener = new AvailabilityListener()
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

        databaseAvailabilityGuard.addListener( availabilityListener );

        // When
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );

        // Then
        assertThat( notified.get(), equalTo( true ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenGrantAndDenyThenNotifyListeners()
    {
        // Given
        Log log = mock( Log.class );
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        final AtomicBoolean notified = new AtomicBoolean();
        AvailabilityListener availabilityListener = new AvailabilityListener()
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

        databaseAvailabilityGuard.addListener( availabilityListener );

        // When
        databaseAvailabilityGuard.fulfill( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        // Then
        assertThat( notified.get(), equalTo( true ) );
    }

    @Test
    public void givenAccessGuardWithConditionWhenShutdownThenInstantlyDenyAccess()
    {
        // Given
        Clock clock = Mockito.mock( Clock.class );
        final DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, NullLog.getInstance() );
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        // When
        databaseAvailabilityGuard.shutdown();

        // Then
        assertFalse( databaseAvailabilityGuard.isAvailable( 1000 ) );
        verifyZeroInteractions( clock );
    }

    @Test
    public void shouldExplainWhoIsBlockingAccess()
    {
        // Given
        Log log = mock( Log.class );
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( clock, log );

        // When
        databaseAvailabilityGuard.require( REQUIREMENT_1 );
        databaseAvailabilityGuard.require( REQUIREMENT_2 );

        // Then
        assertThat( databaseAvailabilityGuard.describeWhoIsBlocking(), equalTo( "2 reasons for blocking: Requirement 1, Requirement 2." ) );
    }

    @Test
    public void shouldExplainBlockersOnCheckAvailable() throws Exception
    {
        // GIVEN
        DatabaseAvailabilityGuard databaseAvailabilityGuard = getDatabaseAvailabilityGuard( Clocks.systemClock(), getInstance() );
        // At this point it should be available
        databaseAvailabilityGuard.checkAvailable();

        // WHEN
        databaseAvailabilityGuard.require( REQUIREMENT_1 );

        // THEN
        try
        {
            databaseAvailabilityGuard.checkAvailable();
            fail( "Should not be available" );
        }
        catch ( UnavailableException e )
        {
            assertThat( e.getMessage(), containsString( REQUIREMENT_1.description() ) );
        }
    }

    private static void verifyLogging( Log log, VerificationMode mode )
    {
        verify( log, mode ).info( anyString(), Mockito.<Object[]>anyVararg() );
    }

    private static DatabaseAvailabilityGuard getDatabaseAvailabilityGuard( Clock clock, Log log )
    {
        return new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, clock, log );
    }
}
