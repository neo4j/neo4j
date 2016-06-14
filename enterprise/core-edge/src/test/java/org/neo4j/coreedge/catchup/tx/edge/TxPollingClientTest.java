/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx.edge;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.CoreClient;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TxPollingClientTest
{
    @Test
    public void shouldPollViaScheduler() throws Exception
    {
        // given
        CoreClient coreClient = mock( CoreClient.class );
        OnDemandJobScheduler scheduler = new OnDemandJobScheduler();

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        TxPollingClient txPollingClient = new TxPollingClient( scheduler, 10, () -> transactionIdStore, coreClient,
                null, mock( CoreServerSelectionStrategy.class ), NullLogProvider.getInstance() );
        txPollingClient.startPolling();

        // when
        scheduler.runJob();

        // then
        verify( coreClient ).pollForTransactions( any( AdvertisedSocketAddress.class ), eq( 14L ) );
    }

    @Test
    public void shouldRegisterTxPullListener() throws Exception
    {
        // given
        TxPullResponseListener listener = mock( TxPullResponseListener.class );
        CoreClient coreClient = mock( CoreClient.class );

        TxPollingClient txPollingClient = new TxPollingClient( new OnDemandJobScheduler(), 10,
                () -> mock( TransactionIdStore.class ), coreClient, listener,
                mock( CoreServerSelectionStrategy.class ), NullLogProvider.getInstance() );

        // when
        txPollingClient.startPolling();

        // then
        verify( coreClient ).addTxPullResponseListener( listener );
    }

    @Test
    public void pollJobShouldLogExceptions() throws Exception
    {
        // given
        RuntimeException exception = new RuntimeException( "Deliberate" );

        CoreClient coreClient = mock( CoreClient.class );
        doThrow( exception ).when( coreClient )
                .pollForTransactions( any( AdvertisedSocketAddress.class ), anyLong() );

        OnDemandJobScheduler scheduler = new OnDemandJobScheduler();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        TxPollingClient txPollingClient = new TxPollingClient( scheduler, 10, () -> mock( TransactionIdStore.class ),
                coreClient, null, mock( CoreServerSelectionStrategy.class ), assertableLogProvider );
        txPollingClient.startPolling();

        // when
        scheduler.runJob();

        // then
        assertableLogProvider.assertAtLeastOnce( inLog( TxPollingClient.class )
                .warn( equalTo( "Tx pull attempt failed, will retry at the next regularly scheduled polling attempt." ),
                        equalTo( exception ) ) );
    }
}