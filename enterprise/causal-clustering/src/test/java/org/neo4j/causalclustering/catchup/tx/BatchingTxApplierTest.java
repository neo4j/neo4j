/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.tx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class BatchingTxApplierTest
{
    private final TransactionIdStore idStore = mock( TransactionIdStore.class );
    private final TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

    private final long startTxId = 31L;
    private final int maxBatchSize = 16;

    private final BatchingTxApplier txApplier = new BatchingTxApplier(
            maxBatchSize, () -> idStore, () -> commitProcess, new Monitors(), PageCursorTracerSupplier.NULL,
            EmptyVersionContextSupplier.EMPTY, NullLogProvider.getInstance() );

    @Before
    public void before()
    {
        when( idStore.getLastCommittedTransactionId() ).thenReturn( startTxId );
        txApplier.start();
    }

    @After
    public void after()
    {
        txApplier.stop();
    }

    @Test
    public void shouldHaveCorrectDefaults()
    {
        assertEquals( startTxId, txApplier.lastQueuedTxId() );
    }

    @Test
    public void shouldApplyBatch() throws Exception
    {
        // given
        txApplier.queue( createTxWithId( startTxId + 1 ) );
        txApplier.queue( createTxWithId( startTxId + 2 ) );
        txApplier.queue( createTxWithId( startTxId + 3 ) );

        // when
        txApplier.applyBatch();

        // then
        assertEquals( startTxId + 3, txApplier.lastQueuedTxId() );
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
        txApplier.applyBatch();

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
        txApplier.applyBatch();

        // then
        assertTransactionsCommitted( startTxId + 1, maxBatchSize );
    }

    @Test( timeout = 3_000 )
    public void shouldGiveUpQueueingOnStop() throws Throwable
    {
        // given
        for ( int i = 1; i <= maxBatchSize; i++ ) // fell the queue
        {
            txApplier.queue( createTxWithId( startTxId + i ) );
        }

        // when
        CountDownLatch latch = new CountDownLatch( 1 );
        Thread thread = new Thread( () -> {
            latch.countDown();
            try
            {
                txApplier.queue( createTxWithId( startTxId + maxBatchSize + 1 ) );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        } );

        thread.start();

        latch.await();
        txApplier.stop();

        // then we don't get stuck
        thread.join();
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
