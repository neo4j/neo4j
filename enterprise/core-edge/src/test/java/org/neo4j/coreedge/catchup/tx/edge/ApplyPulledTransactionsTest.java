/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.function.Suppliers.singleton;

public class ApplyPulledTransactionsTest
{
    @Test
    public void shouldApplyTransaction() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 1, 1, 1 );

        TransactionApplier transactionApplier = mock( TransactionApplier.class );

        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn( 2L );


        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        LogEntryCommit commitEntry = mock(LogEntryCommit.class);
        when( commitEntry.getTxId() ).thenReturn( 3L );
        when( tx.getCommitEntry() ).thenReturn( commitEntry );

        // when
        ApplyPulledTransactions handler = new ApplyPulledTransactions(
                NullLogProvider.getInstance(), singleton( transactionApplier ), singleton(transactionIdStore) );
        handler.onTxReceived( new TxPullResponse( storeId, tx ) );

        // then
        verify( transactionApplier ).appendToLogAndApplyToStore( any( CommittedTransactionRepresentation.class ) );
    }

    @Test
    public void shouldLogIfTransactionCannotBeApplied() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 1, 1, 1 );

        TransactionApplier transactionApplier = mock( TransactionApplier.class );
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        doThrow( TransactionFailureException.class ).when( transactionApplier ).appendToLogAndApplyToStore( any(
                CommittedTransactionRepresentation.class ) );

        LogProvider logProvider = mock( LogProvider.class );
        Log log = mock( Log.class );
        when( logProvider.getLog( ApplyPulledTransactions.class ) ).thenReturn( log );


        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        LogEntryCommit commitEntry = mock(LogEntryCommit.class);
        when( commitEntry.getTxId() ).thenReturn( 3L );
        when( tx.getCommitEntry() ).thenReturn( commitEntry );
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn( 2L );


        // when
        ApplyPulledTransactions handler = new ApplyPulledTransactions(
                logProvider, singleton( transactionApplier ), singleton(transactionIdStore) );
        handler.onTxReceived( new TxPullResponse( storeId, tx ) );

        // then
        verify( log ).error( anyString(), any( Throwable.class ) );
    }

    @Test
    public void shouldNotApplyTransactionsThatHaveAlreadyBeenApplied() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 1, 1, 1 );

        TransactionApplier transactionApplier = mock( TransactionApplier.class );

        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn( 3L );

        CommittedTransactionRepresentation alreadyAppliedTx = mock( CommittedTransactionRepresentation.class );
        LogEntryCommit commitEntry = mock(LogEntryCommit.class);
        when( commitEntry.getTxId() ).thenReturn( 3L );
        when( alreadyAppliedTx.getCommitEntry() ).thenReturn( commitEntry );

        // when
        ApplyPulledTransactions handler = new ApplyPulledTransactions(
                NullLogProvider.getInstance(), singleton( transactionApplier ), singleton(transactionIdStore) );

        handler.onTxReceived( new TxPullResponse( storeId, alreadyAppliedTx ) );

        // then
        verifyNoMoreInteractions( transactionApplier );
    }
}
