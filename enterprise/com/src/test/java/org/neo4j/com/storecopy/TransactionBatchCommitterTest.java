/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.Collections.emptyList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class TransactionBatchCommitterTest
{
    private final KernelTransactions kernelTransactions = mock( KernelTransactions.class );
    private final TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldCommitSmallBatch() throws Exception
    {
        // given
        long safeZone = 10;
        TransactionBatchCommitter committer = newBatchCommitter( safeZone );

        TransactionChain chain = createTxChain( 3, 1, 1 );

        // when
        committer.apply( chain.first, chain.last );

        // then
        verify( commitProcess ).commit( eq( chain.first ), any(), eq( EXTERNAL ) );
    }

    @Test
    public void shouldCommitLargeBatch() throws Exception
    {
        // given
        long safeZone = 10;
        TransactionBatchCommitter committer = newBatchCommitter( safeZone );

        TransactionChain chain = createTxChain( 100, 1, 10 );

        // when
        committer.apply( chain.first, chain.last );

        // then
        verify( commitProcess ).commit( eq( chain.first ), any(), eq( EXTERNAL ) );
    }

    @Test
    public void shouldNotBlockTransactionsForSmallBatch() throws Exception
    {
        // given
        long safeZone = 10;
        TransactionBatchCommitter committer = newBatchCommitter( safeZone );

        TransactionChain chain = createTxChain( 3, 1, 1 );

        // when
        committer.apply( chain.first, chain.last );

        // then
        verify( kernelTransactions, never() ).blockNewTransactions();
        verify( kernelTransactions, never() ).unblockNewTransactions();
    }

    @Test
    public void shouldBlockTransactionsForLargeBatch() throws Exception
    {
        // given
        long safeZone = 10;
        TransactionBatchCommitter committer = newBatchCommitter( safeZone );

        TransactionChain chain = createTxChain( 100, 1, 10 );

        // when
        committer.apply( chain.first, chain.last );

        // then
        InOrder inOrder = inOrder( kernelTransactions );
        inOrder.verify( kernelTransactions ).blockNewTransactions();
        inOrder.verify( kernelTransactions ).unblockNewTransactions();
    }

    @Test
    public void shouldTerminateOutdatedTransactions() throws Exception
    {
        // given
        long safeZone = 10;
        int txCount = 3;
        long firstCommitTimestamp = 10;
        long commitTimestampInterval = 2;
        TransactionBatchCommitter committer = newBatchCommitter( safeZone );

        TransactionChain chain = createTxChain( txCount, firstCommitTimestamp, commitTimestampInterval );
        long timestampOutsideSafeZone =
                chain.last.transactionRepresentation().getLatestCommittedTxWhenStarted() - safeZone - 1;
        KernelTransaction txToTerminate = newKernelTransaction( timestampOutsideSafeZone );
        KernelTransaction tx = newKernelTransaction( firstCommitTimestamp - 1 );

        when( kernelTransactions.activeTransactions() )
                .thenReturn( Iterators.asSet( newHandle( txToTerminate ), newHandle( tx ) ) );

        // when
        committer.apply( chain.first, chain.last );

        // then
        verify( txToTerminate ).markForTermination( Status.Transaction.Outdated );
        verify( tx, never() ).markForTermination( any() );
        logProvider.assertContainsLogCallContaining( "Marking transaction for termination" );
        logProvider.assertContainsLogCallContaining( "lastCommittedTxId:" + (BASE_TX_ID + txCount - 1) );
    }

    private KernelTransactionHandle newHandle( KernelTransaction tx )
    {
        return new TestKernelTransactionHandle( tx );
    }

    private KernelTransaction newKernelTransaction( long lastTransactionTimestampWhenStarted )
    {
        KernelTransaction txToTerminate = mock( KernelTransaction.class );
        when( txToTerminate.lastTransactionTimestampWhenStarted() ).thenReturn( lastTransactionTimestampWhenStarted );
        return txToTerminate;
    }

    private TransactionBatchCommitter newBatchCommitter( long safeZone )
    {
        return new TransactionBatchCommitter( kernelTransactions, safeZone, commitProcess,
                logProvider.getLog( TransactionBatchCommitter.class ) );
    }

    private TransactionChain createTxChain( int txCount, long firstCommitTimestamp, long commitTimestampInterval )
    {
        TransactionToApply first = null;
        TransactionToApply last = null;
        long commitTimestamp = firstCommitTimestamp;
        for ( long i = BASE_TX_ID; i < BASE_TX_ID + txCount; i++ )
        {
            TransactionToApply tx = tx( i, commitTimestamp );
            if ( first == null )
            {
                first = tx;
                last = tx;
            }
            else
            {
                last.next( tx );
                last = tx;
            }
            commitTimestamp += commitTimestampInterval;
        }
        return new TransactionChain( first, last );
    }

    private TransactionToApply tx( long id, long commitTimestamp )
    {
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( emptyList() );
        representation.setHeader( new byte[0], 0, 0, commitTimestamp - 10, id - 1, commitTimestamp, 0 );
        return new TransactionToApply( representation, id );
    }

    private class TransactionChain
    {
        final TransactionToApply first;
        final TransactionToApply last;

        private TransactionChain( TransactionToApply first, TransactionToApply last )
        {
            this.first = first;
            this.last = last;
        }
    }
}
