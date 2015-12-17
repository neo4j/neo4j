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
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.Dependencies;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifeRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_TX_HANDLER;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TransactionCommittingResponseUnpackerTest
{
    public final @Rule LifeRule life = new LifeRule( true );

    @Test
    public void shouldAwaitTransactionObligationsToBeFulfilled() throws Throwable
    {
        // GIVEN
        Dependencies dependencies = mock( Dependencies.class );
        TransactionObligationFulfiller fulfiller = mock( TransactionObligationFulfiller.class );
        when( dependencies.obligationFulfiller() ).thenReturn( fulfiller );
        when( dependencies.logService() ).thenReturn( NullLogService.getInstance() );
        TransactionCommittingResponseUnpacker unpacker =
                life.add( new TransactionCommittingResponseUnpacker( dependencies, 10 ) );

        // WHEN
        unpacker.unpackResponse( new DummyObligationResponse( 4 ), NO_OP_TX_HANDLER );

        // THEN
        verify( fulfiller, times( 1 ) ).fulfill( 4l );
    }

    @Test
    public void shouldCommitTransactionsInBatches() throws Exception
    {
        // GIVEN
        Dependencies dependencies = mock( Dependencies.class );
        TransactionCountingTransactionCommitProcess commitProcess = new TransactionCountingTransactionCommitProcess();
        when( dependencies.commitProcess() ).thenReturn( commitProcess );
        when( dependencies.logService() ).thenReturn( NullLogService.getInstance() );
        TransactionCommittingResponseUnpacker unpacker =
                life.add( new TransactionCommittingResponseUnpacker( dependencies, 5 ) );

        // WHEN
        unpacker.unpackResponse( new DummyTransactionResponse( BASE_TX_ID + 1, 7 ), NO_OP_TX_HANDLER );

        // THEN
        commitProcess.assertBatchSize( 5 );
        commitProcess.assertBatchSize( 2 );
        commitProcess.assertNoMoreBatches();
    }

    private static class DummyObligationResponse extends TransactionObligationResponse<Object>
    {
        public DummyObligationResponse( long obligationTxId )
        {
            super( new Object(), StoreId.DEFAULT, obligationTxId, ResourceReleaser.NO_OP );
        }
    }

    private static class DummyTransactionResponse extends TransactionStreamResponse<Object>
    {
        private final long startingAtTxId;
        private final int txCount;

        public DummyTransactionResponse( long startingAtTxId, int txCount )
        {
            super( new Object(), StoreId.DEFAULT, mock( TransactionStream.class ), ResourceReleaser.NO_OP );
            this.startingAtTxId = startingAtTxId;
            this.txCount = txCount;
        }

        private CommittedTransactionRepresentation tx( long id )
        {
            return new CommittedTransactionRepresentation(
                    new LogEntryStart( 0, 0, 0, 0, new byte[0], UNSPECIFIED ),
                    Commands.transactionRepresentation(),
                    new OnePhaseCommit( id, 0 ) );
        }

        @Override
        public void accept( Response.Handler handler ) throws Exception
        {
            for ( int i = 0; i < txCount; i++ )
            {
                handler.transactions().visit( tx( startingAtTxId + i ) );
            }
        }
    }

    public class TransactionCountingTransactionCommitProcess implements TransactionCommitProcess
    {
        private final Queue<Integer> batchSizes = new LinkedList<>();

        @Override
        public long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
                throws TransactionFailureException
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
