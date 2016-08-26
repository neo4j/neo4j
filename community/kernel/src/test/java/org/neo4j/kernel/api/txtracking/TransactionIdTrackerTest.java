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
package org.neo4j.kernel.api.txtracking;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TransactionIdTrackerTest
{
    private final TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );

    @Test( timeout = 500 )
    public void shouldAlwaysReturnIfTheRequestVersionIsBaseTxIdOrLess() throws Exception
    {
        // given
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( -1L );
        TransactionIdTracker transactionIdTracker =
                new TransactionIdTracker( transactionIdStore );

        // when
        transactionIdTracker.awaitUpToDate( BASE_TX_ID, ofSeconds( 5 ) );

        // then all good!
    }

    @Test( timeout = 500 )
    public void shouldReturnIfTheVersionIsUpToDate() throws Exception
    {
        // given
        long version = 5L;
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( version );
        TransactionIdTracker transactionIdTracker =
                new TransactionIdTracker( transactionIdStore );

        // when
        transactionIdTracker.awaitUpToDate( version, ofSeconds( 5 ) );

        // then all good!

    }

    @Test( timeout = 500 )
    public void shouldTimeoutIfTheVersionIsTooHigh() throws Exception
    {
        // given
        long version = 5L;
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( version );
        TransactionIdTracker transactionIdTracker =
                new TransactionIdTracker( transactionIdStore );

        // when
        try
        {
            transactionIdTracker.awaitUpToDate( version + 1, ofMillis( 50 ) );
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException ex )
        {
            // then all good!
        }
    }
}
