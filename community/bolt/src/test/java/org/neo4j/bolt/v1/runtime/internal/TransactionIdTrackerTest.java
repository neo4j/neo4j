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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, BASE_TX_ID, 5, SECONDS );

        // when
        transactionIdTracking.assertUpToDate();

        // then all good!
    }

    @Test( timeout = 500 )
    public void shouldReturnIfTheVersionIsUpToDate() throws Exception
    {
        // given
        long version = 5L;
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( version );
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, version, 5, SECONDS );

        // when
        transactionIdTracking.assertUpToDate();

        // then all good!

    }

    @Test( timeout = 300 )
    public void shouldTimeoutIfTheVersionIsTooHigh() throws Exception
    {
        // given
        long version = 5L;
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( version );
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, version + 1, 100, MILLISECONDS );

        // when
        try
        {
            transactionIdTracking.assertUpToDate();
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException ex )
        {
            // then all good!
        }
    }

    @Test( timeout = 2000 )
    public void shouldBeKeepCheckingForNewVersionUntilTheTimeoutIsReached() throws Exception
    {
        // given
        long version = 5L;
        when( transactionIdStore.getLastClosedTransactionId() )
                .thenReturn( version, version, version, version, version + 1 );
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, version + 1, 5, SECONDS );

        // when
        transactionIdTracking.assertUpToDate();

        // then all good!
    }

    @Test( timeout = 500 )
    public void shouldBeAbleToUpdateTheVersion() throws Exception
    {
        // given
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( 42L );
        long newVersion = 45L;
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, newVersion, 10, MILLISECONDS );
        try
        {
            transactionIdTracking.assertUpToDate();
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException e )
        {
            // expected
        }

        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( newVersion );

        // when
        transactionIdTracking.updateVersion( 46L );

        try
        {
            transactionIdTracking.assertUpToDate();
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException e )
        {
            // then all good!
        }
    }

    @Test( timeout = 500 )
    public void shouldNotUpdateVersionIfNoPreviousTransactionsInTheDatabase() throws Exception
    {
        // given
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn( 42L );
        final int txIdForEmptyDatabase = -1;
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, txIdForEmptyDatabase, 5, SECONDS );
        transactionIdTracking.assertUpToDate();

        // when
        transactionIdTracking.updateVersion( 46L );

        transactionIdTracking.assertUpToDate();

        // then all good!
    }

    @Test( timeout = 500 )
    public void shouldUpdateVersionUsingTheTransactionIdStoreWhenTheGivenVersionIsBaseTxIdOrLess() throws Exception
    {
        // given
        when( transactionIdStore.getLastClosedTransactionId() ).thenReturn(
                42L, 44L, 43L /* this doesn't make any sense in real life but it helps asserting in this scenario */ );
        TransactionIdTracking transactionIdTracking =
                new TransactionIdTracking( () -> transactionIdStore, 42L, 10, MILLISECONDS );
        transactionIdTracking.assertUpToDate();

        // when
        transactionIdTracking.updateVersion( BASE_TX_ID );

        try
        {
            transactionIdTracking.assertUpToDate();
            fail( "should have thrown" );
        }
        catch ( TransactionFailureException e )
        {
           // then all good!
        }
    }
}
