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

import java.io.File;
import java.io.IOException;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.com.storecopy.ResponseUnpacker.NO_OP_TX_HANDLER;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TransactionCommittingResponseUnpackerTest
{
    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;

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

        TransactionAppender appender = mockedTransactionAppender();
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

        setUpIndexUpdatesValidatorMocking( dependencyResolver );

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
        long committingTransactionId = BASE_TX_ID + 1;
        DummyTransactionResponse response = new DummyTransactionResponse( committingTransactionId, 1, appender, maxBatchSize );
        unpacker.unpackResponse( response, stoppingTxHandler );

        // Then
        // we can't verify transactionCommitted since that's part of the TransactionAppender, which we have mocked
        verify( txIdStore, times( 1 ) ).transactionClosed( committingTransactionId );
        verify( appender, times( 1 ) ).append( any( TransactionRepresentation.class ), anyLong() );
        verify( appender, times( 1 ) ).force();
        verify( logRotation, times( 1 ) ).rotateLogIfNeeded( logAppendEvent );

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

        TransactionAppender appender = mockedTransactionAppender();
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getAppender() ).thenReturn( appender );
        when( dependencyResolver.resolveDependency( LogicalTransactionStore.class ) )
                .thenReturn( logicalTransactionStore );

        when( dependencyResolver.resolveDependency( TransactionRepresentationStoreApplier.class ) )
                .thenReturn( mock( TransactionRepresentationStoreApplier.class ) );

        setUpIndexUpdatesValidatorMocking( dependencyResolver );

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
        verify( logRotation, times( 2 ) ).rotateLogIfNeeded( logAppendEvent );
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
                    new DummyTransactionResponse( BASE_TX_ID+1, 1, appender, 10 ), NO_OP_TX_HANDLER );
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertThat( e.getMessage(), containsString( failure.getMessage() ) );
            verify( kernelHealth ).panic( failure );
        }
    }

    @Test
    public void shouldNotApplyTransactionIfIndexUpdatesValidationFails() throws Throwable
    {
        // Given
        DependencyResolver resolver = mock( DependencyResolver.class );

        when( resolver.resolveDependency( LogFile.class ) ).thenReturn( mock( LogFile.class ) );
        when( resolver.resolveDependency( LogRotation.class ) ).thenReturn( mock( LogRotation.class ) );
        when( resolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( mock( TransactionIdStore.class ) );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        when( resolver.resolveDependency( KernelHealth.class ) ).thenReturn( kernelHealth );
        LogicalTransactionStore txStore = mock( LogicalTransactionStore.class );
        TransactionAppender appender = mockedTransactionAppender();
        when( txStore.getAppender() ).thenReturn( appender );
        when( resolver.resolveDependency( LogicalTransactionStore.class ) ).thenReturn( txStore );
        TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );
        when( resolver.resolveDependency( TransactionRepresentationStoreApplier.class ) ).thenReturn( storeApplier );

        IndexUpdatesValidator validator = mock( IndexUpdatesValidator.class );
        IOException error = new IOException( "error" );
        when( validator.validate( any( TransactionRepresentation.class ), eq( TransactionApplicationMode.EXTERNAL ) ) )
                .thenThrow( error );
        when( resolver.resolveDependency( IndexUpdatesValidator.class ) ).thenReturn( validator );

        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( resolver );
        unpacker.start();

        Response<?> response = new DummyTransactionResponse( BASE_TX_ID + 1, 1, appender, 10 );

        // When
        try
        {
            unpacker.unpackResponse( response, NO_OP_TX_HANDLER );
            fail( "Should have thrown " + IOException.class.getSimpleName() );
        }
        catch ( IOException e )
        {
            assertSame( error, e );
        }

        // Then
        verifyZeroInteractions( storeApplier );
        verify( kernelHealth ).panic( error );
    }

    @Test
    public void shouldNotMarkTransactionsAsCommittedIfAppenderClosed() throws Throwable
    {
        // GIVEN an unpacker with close-to-real dependencies injected
        DependencyResolver resolver = mock( DependencyResolver.class );
        // (we don't want this FS in every test in this class, so just don't use EFSR)
        FileSystemAbstraction fs = cleanup.add( new EphemeralFileSystemAbstraction() );
        File directory = new File( "dir" );
        fs.mkdirs( directory );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory, fs );
        TransactionIdStore transactionIdStore = spy( new DeadSimpleTransactionIdStore() );
        LogVersionRepository logVersionRepository = mock( LogVersionRepository.class );
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 10 );
        LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1_000, transactionIdStore,
                logVersionRepository, new PhysicalLogFile.Monitor.Adapter(), transactionMetadataCache ) );
        KernelHealth health = mock( KernelHealth.class );
        LogRotation logRotation = LogRotation.NO_ROTATION;
        LogicalTransactionStore logicalTransactionStore = life.add( new PhysicalLogicalTransactionStore( logFile,
                logRotation, transactionMetadataCache, transactionIdStore, IdOrderingQueue.BYPASS,
                health ) );
        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        when( indexUpdatesValidator.validate( any( TransactionRepresentation.class ),
                any( TransactionApplicationMode.class ) ) ).thenReturn( ValidatedIndexUpdates.NONE );
        life.start();
        TransactionAppender appender = logicalTransactionStore.getAppender();
        when( resolver.resolveDependency( LogicalTransactionStore.class ) ).thenReturn( logicalTransactionStore );
        when( resolver.resolveDependency( IndexUpdatesValidator.class ) ).thenReturn( indexUpdatesValidator );
        when( resolver.resolveDependency( TransactionIdStore.class ) ).thenReturn( transactionIdStore );
        when( resolver.resolveDependency( TransactionObligationFulfiller.class ) ).thenReturn( null );
        when( resolver.resolveDependency( LogFile.class ) ).thenReturn( logFile );
        when( resolver.resolveDependency( LogRotation.class ) ).thenReturn( logRotation );
        when( resolver.resolveDependency( KernelHealth.class ) ).thenReturn( health );
        when( resolver.resolveDependency( TransactionObligationFulfiller.class ) ).thenThrow(
                new IllegalArgumentException() );
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( resolver );
        unpacker.start();

        // and a closed logFile/appender
        life.shutdown();

        // WHEN packing up a transaction response
        try
        {
            unpacker.unpackResponse( new DummyTransactionResponse( BASE_TX_ID+1, 1, appender, 5 ), NO_OP_TX_HANDLER );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            // THEN apart from failing we don't want any committed/closed calls to TransactionIdStore
            verify( transactionIdStore, times( 0 ) ).transactionCommitted( anyLong(), anyLong() );
            verify( transactionIdStore, times( 0 ) ).transactionClosed( anyLong() );
        }
    }

    private TransactionAppender mockedTransactionAppender() throws IOException
    {
        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ), anyLong() ) ).thenReturn( mock( Commitment.class ) );
        return appender;
    }

    private void setUpIndexUpdatesValidatorMocking( DependencyResolver dependencyResolverMock ) throws IOException
    {
        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );

        doReturn( ValidatedIndexUpdates.NONE )
                .when( indexUpdatesValidator )
                .validate( any( TransactionRepresentation.class ), any( TransactionApplicationMode.class ) );

        doReturn( indexUpdatesValidator )
                .when( dependencyResolverMock )
                .resolveDependency( IndexUpdatesValidator.class );
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
        {   // Nothing to do
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
            when( txRepresentation.additionalHeader() ).thenReturn( new byte[0] );
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

    public final @Rule CleanupRule cleanup = new CleanupRule();
    public final @Rule LifeRule life = new LifeRule();
}
