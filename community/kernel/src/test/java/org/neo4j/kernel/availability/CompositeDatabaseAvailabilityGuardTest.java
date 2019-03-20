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
package org.neo4j.kernel.availability;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.time.Clock;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.internal.NullLogService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class CompositeDatabaseAvailabilityGuardTest
{

    private final DescriptiveAvailabilityRequirement requirement = new DescriptiveAvailabilityRequirement( "testRequirement" );
    private CompositeDatabaseAvailabilityGuard compositeGuard;
    private DatabaseAvailabilityGuard defaultGuard;
    private DatabaseAvailabilityGuard systemGuard;
    private Clock mockClock;

    @BeforeEach
    void setUp() throws Throwable
    {
        mockClock = mock( Clock.class );
        compositeGuard = new CompositeDatabaseAvailabilityGuard( mockClock, NullLogService.getInstance() );
        defaultGuard = compositeGuard.createDatabaseAvailabilityGuard( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        systemGuard = compositeGuard.createDatabaseAvailabilityGuard( new DatabaseId( SYSTEM_DATABASE_NAME ) );
        compositeGuard.start();
    }

    @Test
    void availabilityRequirementOnMultipleGuards()
    {
        assertTrue( defaultGuard.isAvailable() );
        assertTrue( systemGuard.isAvailable() );

        compositeGuard.require( new DescriptiveAvailabilityRequirement( "testRequirement" ) );

        assertFalse( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }

    @Test
    void availabilityFulfillmentOnMultipleGuards()
    {
        compositeGuard.require( requirement );

        assertFalse( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );

        compositeGuard.fulfill( requirement );

        assertTrue( defaultGuard.isAvailable() );
        assertTrue( systemGuard.isAvailable() );
    }

    @Test
    void availableWhenAllGuardsAreAvailable()
    {
        assertTrue( compositeGuard.isAvailable() );

        defaultGuard.require( requirement );

        assertFalse( compositeGuard.isAvailable() );
    }

    @Test
    void compositeGuardDoesNotSupportListeners()
    {
        AvailabilityListener listener = mock( AvailabilityListener.class );
        assertThrows( UnsupportedOperationException.class, () -> compositeGuard.addListener( listener ) );
        assertThrows( UnsupportedOperationException.class, () -> compositeGuard.removeListener( listener ) );
    }

    @Test
    void availabilityTimeoutSharedAcrossAllGuards()
    {
        compositeGuard.require( requirement );
        MutableLong counter = new MutableLong(  );

        when( mockClock.millis() ).thenAnswer( (Answer<Long>) invocation ->
        {
            if ( counter.longValue() == 7 )
            {
                defaultGuard.fulfill( requirement );
            }
            return counter.incrementAndGet();
        } );

        assertFalse( compositeGuard.isAvailable( 10 ) );

        assertThat( counter.getValue(), lessThan( 20L ) );
        assertTrue( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }

    @Test
    void awaitCheckTimeoutSharedAcrossAllGuards()
    {
        compositeGuard.require( requirement );
        MutableLong counter = new MutableLong(  );

        when( mockClock.millis() ).thenAnswer( (Answer<Long>) invocation ->
        {
            if ( counter.longValue() == 7 )
            {
                defaultGuard.fulfill( requirement );
            }
            return counter.incrementAndGet();
        } );

        assertThrows( UnavailableException.class, () -> compositeGuard.await( 10 ) );

        assertThat( counter.getValue(), lessThan( 20L ) );
        assertTrue( defaultGuard.isAvailable() );
        assertFalse( systemGuard.isAvailable() );
    }

    @Test
    void stopOfAvailabilityGuardDeregisterItInCompositeParent()
    {
        int initialGuards = compositeGuard.getGuards().size();
        DatabaseAvailabilityGuard firstGuard = compositeGuard.createDatabaseAvailabilityGuard( new DatabaseId( "first" ) );
        DatabaseAvailabilityGuard secondGuard = compositeGuard.createDatabaseAvailabilityGuard( new DatabaseId( "second" ) );

        assertEquals( 2, countNewGuards( initialGuards ) );

        new Lifespan( firstGuard ).close();

        assertEquals( 1, countNewGuards( initialGuards ) );

        new Lifespan( secondGuard ).close();

        assertEquals( 0, countNewGuards( initialGuards ) );
    }

    @Test
    void compositeGuardIsAvailableByDefault()
    {
        CompositeDatabaseAvailabilityGuard testGuard = new CompositeDatabaseAvailabilityGuard( mockClock, NullLogService.getInstance() );
        assertTrue( testGuard.isAvailable() );
    }

    @Test
    void guardIsShutdownStateAfterStop() throws Throwable
    {
        CompositeDatabaseAvailabilityGuard testGuard = new CompositeDatabaseAvailabilityGuard( mockClock, NullLogService.getInstance() );
        testGuard.start();
        assertFalse( testGuard.isShutdown() );

        testGuard.stop();
        assertTrue( testGuard.isShutdown() );
    }

    @Test
    void stoppedGuardIsNotAvailableInAwait() throws Throwable
    {
        CompositeDatabaseAvailabilityGuard testGuard = new CompositeDatabaseAvailabilityGuard( mockClock, NullLogService.getInstance() );

        testGuard.start();
        assertDoesNotThrow( () -> testGuard.await( 0 ) );

        testGuard.stop();
        assertThrows( UnavailableException.class, () -> testGuard.await( 0 ) );
    }

    private int countNewGuards( int initialGuards )
    {
        return compositeGuard.getGuards().size() - initialGuards;
    }
}
