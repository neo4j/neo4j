/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.junit.Test;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionStream;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransactionCommittingResponseUnpackerTest
{
    /*
     * Tests that shutting down the response unpacker while in the middle of committing a transaction will
     * allow that transaction stream to complete committing. It also verifies that any subsequent transactions
     * won't begin the commit process at all.
     */
    @Test
    public void testStopShouldAllowTransactionsToCompleteCommitAndApply() throws Throwable
    {
        // Given

          // Handcrafted deep mocks, otherwise the dependency resolution throws ClassCastExceptions
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        TransactionIdStore txIdStore = mock( TransactionIdStore.class );

        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );

        TransactionAppender appender = mock( TransactionAppender.class );
          // Should indicate success applying the transaction
        when( appender.append( any( CommittedTransactionRepresentation.class ) ) ).thenReturn( true );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );

          /*
           * The tx handler is called on every transaction applied after setting its id to committing
           * but before setting it to applied. We use this to stop the unpacker in the middle of the
           * process.
           */
        StoppingTxHandler stoppingTxHandler = new StoppingTxHandler();

        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                dependencyResolver );
        stoppingTxHandler.setUnpacker( unpacker );

        // When
        unpacker.start();
        int committingTransactionId = 2;
        DummyResponse response = new DummyResponse( committingTransactionId );
        unpacker.unpackResponse( response, stoppingTxHandler );

        // Then
        verify( txIdStore, times( 1 ) ).transactionCommitted( committingTransactionId );
        verify( txIdStore, times( 1 ) ).transactionClosed( committingTransactionId );
        verify( appender, times( 1 ) ).append( response.getTheTx() );

        // Then
          // The txhandler has stopped the unpacker. It should not allow any more transactions to go through
        try
        {
            unpacker.unpackResponse( mock( Response.class ), stoppingTxHandler );
            fail( "A stopped transaction unpacker should not allow transactions to be applied" );
        }
        catch( IllegalStateException e)
        {
            // good
        }
        verifyNoMoreInteractions( txIdStore );
        verifyNoMoreInteractions( appender );
    }


    private static class StoppingTxHandler implements ResponseUnpacker.TxHandler
    {
        private TransactionCommittingResponseUnpacker unpacker;

        @Override
        public void accept( CommittedTransactionRepresentation tx )
        {
            try
            {
                unpacker.stop();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }

        @Override
        public void done()
        {
        }

        public void setUnpacker( TransactionCommittingResponseUnpacker unpacker )
        {
            this.unpacker = unpacker;
        }
    }

    private static class DummyResponse extends Response<Object>
    {
        private final CommittedTransactionRepresentation theTx;

        public DummyResponse( long txId )
        {
            super( new Object(), StoreId.DEFAULT, mock( TransactionStream.class ), ResourceReleaser.NO_OP );
            theTx = mock( CommittedTransactionRepresentation.class );
            LogEntryCommit mockCommitEntry = mock( LogEntryCommit.class );
            when( mockCommitEntry.getTxId() ).thenReturn( txId );
            when( theTx.getCommitEntry() ).thenReturn( mockCommitEntry );
        }

        public CommittedTransactionRepresentation getTheTx()
        {
            return theTx;
        }

        @Override
        public void accept( Visitor<CommittedTransactionRepresentation, IOException> visitor ) throws IOException
        {
            visitor.visit( theTx );
        }


    }
}
