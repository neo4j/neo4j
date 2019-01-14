/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.Dependencies;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.com.storecopy.ResponseUnpacker.TxHandler.NO_OP_TX_HANDLER;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TransactionCommittingResponseUnpackerTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );

    /*
      * Tests that we unfreeze active transactions after commit and after apply of batch if batch length (in time)
      * is larger than safeZone time.
      */
    @Test
    public void shouldUnfreezeKernelTransactionsAfterApplyIfBatchIsLarge() throws Throwable
    {
        // GIVEN
        int maxBatchSize = 10;
        long idReuseSafeZoneTime = 100;
        Dependencies dependencies = mock( Dependencies.class );
        TransactionObligationFulfiller fulfiller = mock( TransactionObligationFulfiller.class );
        when( dependencies.obligationFulfiller() ).thenReturn( fulfiller );
        when( dependencies.logService() ).thenReturn( NullLogService.getInstance() );
        when( dependencies.versionContextSupplier() ).thenReturn( EmptyVersionContextSupplier.EMPTY );
        KernelTransactions kernelTransactions = mock( KernelTransactions.class );
        when( dependencies.kernelTransactions() ).thenReturn( kernelTransactions );
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
        when( dependencies.commitProcess() ).thenReturn( commitProcess );
        TransactionCommittingResponseUnpacker unpacker = life.add(
                new TransactionCommittingResponseUnpacker( dependencies, maxBatchSize, idReuseSafeZoneTime ) );

        // WHEN
        int txCount = maxBatchSize;
        int doesNotMatter = 1;
        unpacker.unpackResponse(
                new DummyTransactionResponse( doesNotMatter, txCount, idReuseSafeZoneTime + 1 ),
                NO_OP_TX_HANDLER );

        // THEN
        InOrder inOrder = inOrder( commitProcess, kernelTransactions );
        inOrder.verify( commitProcess, times( 1 ) ).commit( any(), any(), any() );
        inOrder.verify( kernelTransactions, times( 1 ) ).unblockNewTransactions();
    }

    @Test
    public void shouldAwaitTransactionObligationsToBeFulfilled() throws Throwable
    {
        // GIVEN
        Dependencies dependencies = mock( Dependencies.class );
        TransactionObligationFulfiller fulfiller = mock( TransactionObligationFulfiller.class );
        when( dependencies.obligationFulfiller() ).thenReturn( fulfiller );
        when( dependencies.logService() ).thenReturn( NullLogService.getInstance() );
        TransactionCommittingResponseUnpacker unpacker =
                life.add( new TransactionCommittingResponseUnpacker( dependencies, 10, 0 ) );

        // WHEN
        unpacker.unpackResponse( new DummyObligationResponse( 4 ), NO_OP_TX_HANDLER );

        // THEN
        verify( fulfiller, times( 1 ) ).fulfill( 4L );
    }

    @Test
    public void shouldCommitTransactionsInBatches() throws Exception
    {
        // GIVEN
        Dependencies dependencies = mock( Dependencies.class );
        TransactionCountingTransactionCommitProcess commitProcess = new TransactionCountingTransactionCommitProcess();
        when( dependencies.commitProcess() ).thenReturn( commitProcess );
        when( dependencies.logService() ).thenReturn( NullLogService.getInstance() );
        when( dependencies.versionContextSupplier() ).thenReturn( EmptyVersionContextSupplier.EMPTY );
        KernelTransactions kernelTransactions = mock( KernelTransactions.class );
        when( dependencies.kernelTransactions() ).thenReturn( kernelTransactions );
        TransactionCommittingResponseUnpacker unpacker =
                life.add( new TransactionCommittingResponseUnpacker( dependencies, 5, 0 ) );

        // WHEN
        unpacker.unpackResponse( new DummyTransactionResponse( BASE_TX_ID + 1, 7 ), NO_OP_TX_HANDLER );

        // THEN
        commitProcess.assertBatchSize( 5 );
        commitProcess.assertBatchSize( 2 );
        commitProcess.assertNoMoreBatches();
    }

    private static class DummyObligationResponse extends TransactionObligationResponse<Object>
    {
        DummyObligationResponse( long obligationTxId )
        {
            super( new Object(), StoreId.DEFAULT, obligationTxId, ResourceReleaser.NO_OP );
        }
    }

    private static class DummyTransactionResponse extends TransactionStreamResponse<Object>
    {
        private static final long UNDEFINED_BATCH_LENGTH = -1;

        private final long startingAtTxId;
        private final int txCount;
        private final long batchLength;

        DummyTransactionResponse( long startingAtTxId, int txCount )
        {
            this( startingAtTxId, txCount, UNDEFINED_BATCH_LENGTH );
        }

        DummyTransactionResponse( long startingAtTxId, int txCount, long batchLength )
        {
            super( new Object(), StoreId.DEFAULT, mock( TransactionStream.class ), ResourceReleaser.NO_OP );
            this.startingAtTxId = startingAtTxId;
            this.txCount = txCount;
            this.batchLength = batchLength;
        }

        private CommittedTransactionRepresentation tx( long id, long commitTimestamp )
        {
            PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( emptyList() );
            representation.setHeader( new byte[0], 0, 0, commitTimestamp - 10, id - 1, commitTimestamp, 0 );

            return new CommittedTransactionRepresentation(
                    new LogEntryStart( 0, 0, 0, 0, new byte[0], UNSPECIFIED ),
                    representation,
                    new LogEntryCommit( id, commitTimestamp ) );
        }

        private long timestamp( int txNbr, int txCount, long batchLength )
        {
            if ( txCount == 1 )
            {
                return 0;
            }
            return txNbr * batchLength / (txCount - 1);
        }

        @Override
        public void accept( Response.Handler handler ) throws Exception
        {
            for ( int i = 0; i < txCount; i++ )
            {
                handler.transactions().visit( tx( startingAtTxId + i, timestamp( i, txCount, batchLength ) ) );
            }
        }
    }

    public class TransactionCountingTransactionCommitProcess implements TransactionCommitProcess
    {
        private final Queue<Integer> batchSizes = new LinkedList<>();

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
        {
            int batchSize = count( batch );
            batchSizes.offer( batchSize );
            return 42;
        }

        protected void assertBatchSize( int expected )
        {
            int batchSize = batchSizes.poll();
            assertEquals( expected, batchSize );
        }

        protected void assertNoMoreBatches()
        {
            assertTrue( batchSizes.isEmpty() );
        }

        private int count( TransactionToApply batch )
        {
            int count = 0;
            while ( batch != null )
            {
                count++;
                batch = batch.next();
            }
            return count;
        }
    }
}
