/**
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
package org.neo4j.server.rest.transactional;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.server.rest.transactional.error.InvalidConcurrentTransactionAccess;
import org.neo4j.server.rest.transactional.error.InvalidTransactionId;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;

public class TransactionHandleRegistryTest
{

    @Test
    public void shouldGenerateTransactionId() throws Exception
    {
        // given
        TestLogger log = new TestLogger();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( new FakeClock(), 0, log );

        // when
        long id1 = registry.begin();
        long id2 = registry.begin();

        // then
        assertNotEquals( id1, id2 );
        log.assertNoLoggingOccurred();
    }

    @Test
    public void shouldStoreSuspendedTransaction() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( new FakeClock(), 0, log );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin();

        // When
        registry.release( id, handle );
        TransactionHandle acquiredHandle = registry.acquire( id );

        // Then
        assertSame( handle, acquiredHandle );
        log.assertNoLoggingOccurred();
    }

    @Test
    public void acquiringATransactionThatHasAlreadyBeenAcquiredShouldThrowInvalidConcurrentTransactionAccess() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( new FakeClock(), 0, log );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin();
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
        log.assertNoLoggingOccurred();
    }

    @Test
    public void acquiringANonExistentTransactionShouldThrowErrorInvalidTransactionId() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( new FakeClock(), 0, log );

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
        log.assertNoLoggingOccurred();
    }

    @Test
    public void transactionsShouldBeEvictedWhenUnusedLongerThanTimeout() throws Exception
    {
        // Given
        FakeClock clock = new FakeClock();
        TestLogger log = new TestLogger();
        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, 0, log );
        TransactionHandle oldTx = mock( TransactionHandle.class );
        TransactionHandle newTx = mock( TransactionHandle.class );

        long txId1 = registry.begin();
        long txId2 = registry.begin();

        // And given one transaction was stored one minute ago, and another was stored just now
        registry.release( txId1, oldTx );
        clock.forward( 1, TimeUnit.MINUTES );
        registry.release( txId2, newTx );

        // When
        registry.rollbackSuspendedTransactionsIdleSince( clock.currentTimeMillis() - 1000 );

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

        log.assertExactly( info( "Transaction with id 1 has been automatically rolled back." ) );
    }

    @Test
    public void expiryTimeShouldBeSetToCurrentTimePlusTimeout() throws Exception
    {
        // Given
        TestLogger log = new TestLogger();
        FakeClock clock = new FakeClock();
        int timeoutLength = 123;

        TransactionHandleRegistry registry = new TransactionHandleRegistry( clock, timeoutLength, log );
        TransactionHandle handle = mock( TransactionHandle.class );

        long id = registry.begin();

        // When
        long timesOutAt = registry.release( id, handle );

        // Then
        assertThat( timesOutAt, equalTo( clock.currentTimeMillis() + timeoutLength ) );

        // And when
        clock.forward( 1337, TimeUnit.MILLISECONDS );
        registry.acquire( id );
        timesOutAt = registry.release( id, handle );

        // Then
        assertThat( timesOutAt, equalTo( clock.currentTimeMillis() + timeoutLength ) );
    }
}
