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
package org.neo4j.coreedge.catchup.tx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class BatchingTxApplierTest
{
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
    private final DatabaseHealth dbHealth = mock( DatabaseHealth.class );

    private final long startTxId = 31L;
    private final int maxBatchSize = 16;

    private final BatchingTxApplier txApplier = new BatchingTxApplier( maxBatchSize, () -> idStore, () -> commitProcess,
            () -> dbHealth, new Monitors(), NullLogProvider.getInstance() );

    @Before
    public void before() throws Throwable
    {
        when( idStore.getLastCommittedTransactionId() ).thenReturn( startTxId );
        txApplier.start();
    }

    @After
    public void after() throws Throwable
    {
        txApplier.stop();
    }

    @Test
    public void shouldHaveCorrectDefaults() throws Throwable
    {
        assertEquals( startTxId, txApplier.lastAppliedTxId() );
        assertFalse( txApplier.workPending() );
    }

    @Test
    public void shouldHaveWorkPendingAfterItHasBeenQueued() throws Exception
    {
        // when
        txApplier.queue( createTxWithId( startTxId + 1 ) );

        // then
        assertTrue( txApplier.workPending() );
    }

    @Test
    public void shouldApplyBatch() throws Exception
    {
        // given
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );

        // when
        txApplier.run();

        // then
        assertFalse( txApplier.workPending() );
        assertEquals( startTxId + 3, txApplier.lastAppliedTxId() );
        assertTransactionsCommitted( startTxId + 1, 3 );
    }

    @Test
    public void shouldIgnoreOutOfOrderTransactions() throws Exception
    {
        // given
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );
        txApplier.queue( createTxWithId( startTxId + 5 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 5 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 4 ) );
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 4 ) ); // ignored
        txApplier.queue( createTxWithId( startTxId + 6 ) ); // ignored

        // when
        txApplier.run();

        // then
        assertTransactionsCommitted( startTxId + 1, 4 );
    }

    @Test
    public void shouldBeAbleToQueueMaxBatchSize() throws Exception
    {
        // given
        long endTxId = startTxId + maxBatchSize;
        for ( long txId = startTxId + 1; txId <= endTxId; txId++ )
        {
            txApplier.queue( createTxWithId( txId ) );
        }

        // when
        txApplier.run();

        // then
        assertTransactionsCommitted( startTxId + 1, maxBatchSize );
    }

    @Test
    public void shouldPanicIfTransactionFailsToApply() throws Throwable
    {
        // given
        doThrow( Exception.class ).when( commitProcess ).commit( any(), any(), any() );
        txApplier.queue( createTxWithId( startTxId + 1 ) );

        // when
        txApplier.run();

        // then
        verify( dbHealth ).panic( any() );
    }

    private CommittedTransactionRepresentation createTxWithId( long txId )
    {
        CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
        LogEntryCommit commitEntry = mock( LogEntryCommit.class );
        when( commitEntry.getTxId() ).thenReturn( txId );
        TransactionRepresentation txRep = mock( TransactionRepresentation.class );
        when( tx.getTransactionRepresentation() ).thenReturn( txRep );
        when( tx.getCommitEntry() ).thenReturn( commitEntry );
        return tx;
    }

    private void assertTransactionsCommitted( long startTxId, long expectedCount ) throws TransactionFailureException
    {
        ArgumentCaptor<TransactionToApply> batchCaptor = ArgumentCaptor.forClass( TransactionToApply.class );
        verify( commitProcess ).commit( batchCaptor.capture(), eq( NULL ), eq( EXTERNAL ) );

        TransactionToApply batch = Iterables.single( batchCaptor.getAllValues() );
        long expectedTxId = startTxId;
        long count = 0;
        while ( batch != null )
        {
            assertEquals( expectedTxId, batch.transactionId() );
            expectedTxId++;
            batch = batch.next();
            count++;
        }
        assertEquals( expectedCount, count );
    }
}
