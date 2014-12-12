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
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.test.DoubleLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_TX_HANDLER;

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
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );
        LogFile logFile = mock( LogFile.class );
        when( dependencyResolver.resolveDependency( LogFile.class ) ).thenReturn( logFile );
        LogRotation logRotation = mock(LogRotation.class);
        when( dependencyResolver.resolveDependency( LogRotation.class ) ).thenReturn( logRotation );

          /*
           * The tx handler is called on every transaction applied after setting its id to committing
           * but before setting it to applied. We use this to stop the unpacker in the middle of the
           * process.
           */
        StoppingTxHandler stoppingTxHandler = new StoppingTxHandler();

        int maxBatchSize = 10;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                dependencyResolver, maxBatchSize );
        stoppingTxHandler.setUnpacker( unpacker );

        // When
        unpacker.start();
        long committingTransactionId = TransactionIdStore.BASE_TX_ID + 1;
        DummyTransactionResponse response = new DummyTransactionResponse( committingTransactionId, 1, appender, maxBatchSize );
        unpacker.unpackResponse( response, stoppingTxHandler );

        // Then
        verify( txIdStore, times( 1 ) ).transactionCommitted( committingTransactionId, 0 );
        verify( txIdStore, times( 1 ) ).transactionClosed( committingTransactionId );
        verify( appender, times( 1 ) ).append( any( TransactionRepresentation.class ), anyLong() );
        verify( appender, times( 1 ) ).force();
        verify( logRotation, times( 1 ) ).rotateLogIfNeeded();

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

    @Test
    public void shouldApplyQueuedTransactionsIfMany() throws Throwable
    {
        // GIVEN
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        TransactionIdStore txIdStore = mock( TransactionIdStore.class );

        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );

        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ) ) ).thenReturn( 2L, 3L, 4L, 5L, 6L );
          // Should indicate success applying the transaction

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );

        LogFile logFile = mock( LogFile.class );
        when( dependencyResolver.resolveDependency( LogFile.class ) ).thenReturn( logFile );

        LogRotation logRotation = mock(LogRotation.class);
        when( dependencyResolver.resolveDependency( LogRotation.class ) ).thenReturn( logRotation );

        int maxBatchSize = 3;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                dependencyResolver, maxBatchSize );
        unpacker.start();

        // WHEN/THEN
        int txCount = maxBatchSize * 2 - 1;
        unpacker.unpackResponse( new DummyTransactionResponse( 2, txCount, appender, maxBatchSize ), NO_OP_TX_HANDLER );

        // and THEN
        verify( appender, times( txCount ) ).append( any( TransactionRepresentation.class ), anyLong() );
        verify( appender, times( 2 ) ).force();
        verify( logRotation, times( 2 ) ).rotateLogIfNeeded();
    }

    @Test
    public void shouldAwaitTransactionObligationsToBeFulfilled() throws Throwable
    {
        // GIVEN
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );

        TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );

        TransactionAppender appender = mock( TransactionAppender.class );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );
        TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        when( dependencyResolver.resolveDependency( TransactionObligationFulfiller.class ) )
                .thenReturn( obligationFulfiller );
        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                dependencyResolver );
        unpacker.start();

        // WHEN
        unpacker.unpackResponse( new DummyObligationResponse( 4 ), NO_OP_TX_HANDLER );

        // THEN
        verify( obligationFulfiller, times( 1 ) ).fulfill( 4l );
    }

    @Test
    public void shouldIssueKernelPanicInCaseOfFailureToAppendOrApply() throws Throwable
    {
        // GIVEN
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );

        TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        when( dependencyResolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( txIdStore );

        TransactionAppender appender = mock( TransactionAppender.class );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );
        TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        when( dependencyResolver.resolveDependency( TransactionObligationFulfiller.class ) )
                .thenReturn( obligationFulfiller );
        LogFile logFile = mock( LogFile.class );
        when( dependencyResolver.resolveDependency( LogFile.class ) ).thenReturn( logFile );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        when( dependencyResolver.resolveDependency( KernelHealth.class ) ).thenReturn( kernelHealth );
        LogRotation logRotation = mock(LogRotation.class);
        when( dependencyResolver.resolveDependency( LogRotation.class ) ).thenReturn( logRotation );
        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                dependencyResolver );
        unpacker.start();

        // WHEN failing to append one or more transactions from a transaction stream response
        IOException failure = new IOException( "Expected failure" );
        doThrow( failure ).when( appender ).append( any( TransactionRepresentation.class ), anyLong() );
        try
        {
            unpacker.unpackResponse(
                    new DummyTransactionResponse( TransactionIdStore.BASE_TX_ID+1, 1, appender, 10 ), NO_OP_TX_HANDLER );
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertThat( e.getMessage(), containsString( failure.getMessage() ) );
            verify( kernelHealth ).panic( failure );
        }
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
        private final TransactionAppender appender;
        private final int maxBatchSize;

        public DummyTransactionResponse( long startingAtTxId, int txCount, TransactionAppender appender, int maxBatchSize )
        {
            super( new Object(), StoreId.DEFAULT, mock( TransactionStream.class ), ResourceReleaser.NO_OP );
            this.startingAtTxId = startingAtTxId;
            this.txCount = txCount;
            this.appender = appender;
            this.maxBatchSize = maxBatchSize;
        }

        private CommittedTransactionRepresentation tx( long id )
        {
            CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
            LogEntryCommit mockCommitEntry = mock( LogEntryCommit.class );
            when( mockCommitEntry.getTxId() ).thenReturn( id );
            when( tx.getCommitEntry() ).thenReturn( mockCommitEntry );
            LogEntryStart mockStartEntry = mock( LogEntryStart.class );
            when( mockStartEntry.checksum() ).thenReturn( id*10 );
            when( tx.getStartEntry() ).thenReturn( mockStartEntry );
            TransactionRepresentation txRepresentation = mock( TransactionRepresentation.class );
            when( tx.getTransactionRepresentation() ).thenReturn( txRepresentation );
            return tx;
        }

        @Override
        public void accept( Response.Handler handler ) throws IOException
        {
            for ( int i = 0; i < txCount; i++ )
            {
                handler.transactions().visit( tx( startingAtTxId+i ) );
                if ( (i+1) % maxBatchSize == 0 )
                {
                    try
                    {
                        verify( appender, times( maxBatchSize ) ).append( any( TransactionRepresentation.class ), anyLong() );
                        verify( appender, times( 1 ) ).force();
                    }
                    catch ( IOException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
                else
                {
                    verifyNoMoreInteractions( appender );
                }
            }
        }
    }

    public class ControlledObligationFulfuller implements TransactionObligationFulfiller
    {
        private final DoubleLatch latch;

        public ControlledObligationFulfuller( DoubleLatch latch )
        {
            this.latch = latch;
        }

        @Override
        public void fulfill( long toTxId ) throws InterruptedException
        {
            latch.startAndAwaitFinish();
        }
    }
}
