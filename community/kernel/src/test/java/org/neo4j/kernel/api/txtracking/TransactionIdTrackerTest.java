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
package org.neo4j.kernel.api.txtracking;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TransactionIdTrackerTest
{
    private static final Duration DEFAULT_DURATION = ofSeconds( 10 );

    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private final AvailabilityGuard availabilityGuard = mock( AvailabilityGuard.class );

    private TransactionIdTracker transactionIdTracker;

    @Before
    public void setup()
    {
        when( availabilityGuard.isAvailable() ).thenReturn( true );
        transactionIdTracker = new TransactionIdTracker( () -> transactionIdStore, availabilityGuard );
    }

    @Test
    public void shouldReturnImmediatelyForBaseTxIdOrLess() throws Exception
    {
        // when
        transactionIdTracker.awaitUpToDate( BASE_TX_ID, ofSeconds( 5 ) );

        // then
        verify( transactionIdStore, never() ).awaitClosedTransactionId( anyLong(), anyLong() );
    }

    @Test
    public void shouldWaitForRequestedVersion() throws Exception
    {
        // given
        long version = 5L;

        // when
        transactionIdTracker.awaitUpToDate( version, DEFAULT_DURATION );

        // then
        verify( transactionIdStore ).awaitClosedTransactionId( version, DEFAULT_DURATION.toMillis() );
    }

    @Test
    public void shouldPropagateTimeoutException() throws Exception
    {
        // given
        long version = 5L;
        TimeoutException timeoutException = new TimeoutException();
        doThrow( timeoutException ).when( transactionIdStore ).awaitClosedTransactionId( anyLong(), anyLong() );

        try
        {
            // when
            transactionIdTracker.awaitUpToDate( version + 1, ofMillis( 50 ) );
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException ex )
        {
            // then
            assertEquals( Status.Transaction.InstanceStateChanged, ex.status() );
            assertEquals( timeoutException, ex.getCause() );
        }
    }

    @Test
    public void shouldNotWaitIfTheDatabaseIsUnavailable() throws Exception
    {
        // given
        when( availabilityGuard.isAvailable() ).thenReturn( false );

        try
        {
            // when
            transactionIdTracker.awaitUpToDate( 1000, ofMillis( 60_000 ) );
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException ex )
        {
            // then
            assertEquals( Status.General.DatabaseUnavailable, ex.status() );
        }

        verify( transactionIdStore, never() ).awaitClosedTransactionId( anyLong(), anyLong() );
    }

    @Test
    public void shouldReturnNewestTransactionId()
    {
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( 42L );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 4242L );

        assertEquals( 4242L, transactionIdTracker.newestEncounteredTxId() );
    }
}
