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
package org.neo4j.server.rest.transactional;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.rest.transactional.error.InvalidConcurrentTransactionAccess;
import org.neo4j.server.rest.transactional.error.InvalidTransactionId;
import org.neo4j.server.rest.transactional.error.TransactionLifecycleException;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TransactionHandleRegistryTest
{
    @Test
    public void shouldGenerateTransactionId()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( Clocks.fakeClock(), 0, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        // when
        long id1 = registry.begin( handle );
        long id2 = registry.begin( handle );

        // then
        assertNotEquals( id1, id2 );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldStoreSuspendedTransaction() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( Clocks.fakeClock(), 0, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin( handle );

        // When
        registry.release( id, handle );
        TransactionHandle acquiredHandle = registry.acquire( id );

        // Then
        assertSame( handle, acquiredHandle );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void acquiringATransactionThatHasAlreadyBeenAcquiredShouldThrowInvalidConcurrentTransactionAccess() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( Clocks.fakeClock(), 0, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin( handle );
        registry.release( id, handle );
        registry.acquire( id );

        // When
        try
        {
            registry.acquire( id );
            fail( "Should have thrown exception" );
        }
        catch ( InvalidConcurrentTransactionAccess e )
        {
            // expected
        }

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void acquiringANonExistentTransactionShouldThrowErrorInvalidTransactionId() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( Clocks.fakeClock(), 0, logProvider );

        long madeUpTransactionId = 1337;

        // When
        try
        {
            registry.acquire( madeUpTransactionId );
            fail( "Should have thrown exception" );
        }
        catch ( InvalidTransactionId e )
        {
            // expected
        }

        // then
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void transactionsShouldBeEvictedWhenUnusedLongerThanTimeout() throws Exception
    {
        // Given
        FakeClock clock = Clocks.fakeClock();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, 0, logProvider );
        TransactionHandle oldTx = mock( TransactionHandle.class );
        TransactionHandle newTx = mock( TransactionHandle.class );
        TransactionHandle handle = mock( TransactionHandle.class );

        long txId1 = registry.begin( handle );
        long txId2 = registry.begin( handle );

        // And given one transaction was stored one minute ago, and another was stored just now
        registry.release( txId1, oldTx );
        clock.forward( 1, TimeUnit.MINUTES );
        registry.release( txId2, newTx );

        // When
        registry.rollbackSuspendedTransactionsIdleSince( clock.millis() - 1000 );

        // Then
        assertThat( registry.acquire( txId2 ), equalTo( newTx ) );

        // And then the other should have been evicted
        try
        {
            registry.acquire( txId1 );
            fail( "Should have thrown exception" );
        }
        catch ( InvalidTransactionId e )
        {
            // ok
        }

        logProvider.assertExactly(
                inLog( TransactionHandleRegistry.class ).info( "Transaction with id 1 has been automatically rolled " +
                        "back due to transaction timeout." )
        );
    }

    @Test
    public void expiryTimeShouldBeSetToCurrentTimePlusTimeout() throws Exception
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        int timeoutLength = 123;

        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, timeoutLength, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin( handle );

        // When
        long timesOutAt = registry.release( id, handle );

        // Then
        assertThat( timesOutAt, equalTo( clock.millis() + timeoutLength ) );

        // And when
        clock.forward( 1337, TimeUnit.MILLISECONDS );
        registry.acquire( id );
        timesOutAt = registry.release( id, handle );

        // Then
        assertThat( timesOutAt, equalTo( clock.millis() + timeoutLength ) );
    }

    @Test
    public void shouldProvideInterruptHandlerForActiveTransaction() throws TransactionLifecycleException
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        int timeoutLength = 123;

        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, timeoutLength, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        // Active Tx in Registry
        long id = registry.begin( handle );

        // When
        registry.terminate( id );

        // Then
        verify( handle, times( 1 ) ).terminate();
        verifyNoMoreInteractions( handle );
    }

    @Test
    public void shouldProvideInterruptHandlerForSuspendedTransaction() throws TransactionLifecycleException
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        int timeoutLength = 123;

        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, timeoutLength, logProvider );
        TransactionHandle handle = mock( TransactionHandle.class );

        // Suspended Tx in Registry
        long id = registry.begin( handle );
        registry.release( id, handle );

        // When
        registry.terminate( id );

        // Then
        verify( handle, times( 1 ) ).terminate();
        verifyNoMoreInteractions( handle );
    }

    @Test( expected = InvalidTransactionId.class )
    public void gettingInterruptHandlerForUnknownIdShouldThrowErrorInvalidTransactionId() throws TransactionLifecycleException
    {
        // Given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        FakeClock clock = Clocks.fakeClock();
        int timeoutLength = 123;

        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, timeoutLength, logProvider );

        // When
        registry.terminate( 456 );
    }
}
