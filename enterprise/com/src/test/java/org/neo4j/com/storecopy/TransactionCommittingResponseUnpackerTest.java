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
import org.neo4j.function.Consumers;
import org.neo4j.function.LongConsumer;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
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
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
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
    public final @Rule CleanupRule cleanup = new CleanupRule();
    public final @Rule LifeRule life = new LifeRule();

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
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        final TransactionAppender appender = mockedTransactionAppender();
        final LogFile logFile = mock( LogFile.class );
        final LogRotation logRotation = mock( LogRotation.class );
        final TransactionRepresentationStoreApplier applier = mock(TransactionRepresentationStoreApplier.class);
        final IndexUpdatesValidator indexUpdatesValidator = setUpIndexUpdatesValidatorMocking(  );

          /*
           * The tx handler is called on every transaction applied after setting its id to committing
           * but before setting it to applied. We use this to stop the unpacker in the middle of the
           * process.
           */
        StoppingTxHandler stoppingTxHandler = new StoppingTxHandler();

        TransactionCommittingResponseUnpacker.Dependencies deps = buildDependencies( txIdStore, logFile,
                mock(KernelHealth.class), logRotation,
                indexUpdatesValidator, applier, appender, mock(TransactionObligationFulfiller.class) );

        int maxBatchSize = 10;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(deps, maxBatchSize );
        stoppingTxHandler.setUnpacker( unpacker );

        // When
        unpacker.start();
        long committingTransactionId = BASE_TX_ID + 1;
        DummyTransactionResponse response = new DummyTransactionResponse( committingTransactionId, 1, appender,
                maxBatchSize );
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
        catch ( IllegalStateException e )
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
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        final TransactionRepresentationStoreApplier applier = mock(TransactionRepresentationStoreApplier.class);
        final TransactionAppender appender = mockedTransactionAppender();
        final IndexUpdatesValidator indexUpdatesValidator = setUpIndexUpdatesValidatorMocking(  );
        final LogFile logFile = mock( LogFile.class );
        final LogRotation logRotation = mock(LogRotation.class);

        TransactionCommittingResponseUnpacker.Dependencies deps = buildDependencies( txIdStore, logFile, null,
                logRotation, indexUpdatesValidator, applier, appender, mock(TransactionObligationFulfiller.class) );

        int maxBatchSize = 3;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(deps, maxBatchSize );
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
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        final TransactionAppender appender = mock( TransactionAppender.class );
        final TransactionRepresentationStoreApplier applier = mock(TransactionRepresentationStoreApplier.class);
        final TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );

        TransactionCommittingResponseUnpacker.Dependencies deps = buildDependencies( txIdStore, mock(LogFile.class),
                mock(KernelHealth.class), mock(LogRotation.class),
                mock(IndexUpdatesValidator.class), applier, appender, obligationFulfiller );

        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(deps);
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
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        final TransactionAppender appender = mock( TransactionAppender.class );
        final TransactionRepresentationStoreApplier applier = mock(TransactionRepresentationStoreApplier.class);
        final TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        final LogFile logFile = mock( LogFile.class );
        final KernelHealth kernelHealth = mock( KernelHealth.class );
        final LogRotation logRotation = mock( LogRotation.class );

        TransactionCommittingResponseUnpacker.Dependencies deps = buildDependencies( txIdStore,
                logFile, kernelHealth, logRotation
                , mock(IndexUpdatesValidator.class), applier, appender, obligationFulfiller );

        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps );
        unpacker.start();

        // WHEN failing to append one or more transactions from a transaction stream response
        IOException failure = new IOException( "Expected failure" );
        doThrow( failure ).when( appender ).append( any( TransactionRepresentation.class ), anyLong() );
        try
        {
            unpacker.unpackResponse(
                    new DummyTransactionResponse( BASE_TX_ID + 1, 1, appender, 10 ), NO_OP_TX_HANDLER );
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
        final KernelHealth kernelHealth = mock( KernelHealth.class );
        final TransactionAppender appender = mockedTransactionAppender();
        final TransactionRepresentationStoreApplier storeApplier = mock( TransactionRepresentationStoreApplier.class );
        final IndexUpdatesValidator validator = mock( IndexUpdatesValidator.class );
        IOException error = new IOException( "error" );
        when( validator.validate( any( TransactionRepresentation.class ), eq( TransactionApplicationMode.EXTERNAL ) ) )
                .thenThrow( error );

        TransactionCommittingResponseUnpacker.Dependencies deps = buildDependencies( mock(TransactionIdStore.class),
            mock(LogFile.class), kernelHealth, mock(LogRotation.class), validator, storeApplier, appender,
                mock( TransactionObligationFulfiller.class ) );

        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps );
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
        // (we don't want this FS in every test in this class, so just don't use EFSR)
        FileSystemAbstraction fs = cleanup.add( new EphemeralFileSystemAbstraction() );
        File directory = new File( "dir" );
        fs.mkdirs( directory );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( directory, fs );
        final TransactionIdStore transactionIdStore = spy( new DeadSimpleTransactionIdStore() );
        LogVersionRepository logVersionRepository = mock( LogVersionRepository.class );
        TransactionMetadataCache transactionMetadataCache = new TransactionMetadataCache( 10, 10 );
        final LogFile logFile = life.add( new PhysicalLogFile( fs, logFiles, 1_000, transactionIdStore,
                logVersionRepository, new PhysicalLogFile.Monitor.Adapter(), transactionMetadataCache ) );
        final KernelHealth health = mock( KernelHealth.class );
        final LogRotation logRotation = LogRotation.NO_ROTATION;
        final CheckPointer checkPointer = CheckPointer.NO_CHECKPOINT;
        LongConsumer transactionCommittedConsumer = Consumers.LNOOP;
        final IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        when( indexUpdatesValidator.validate( any( TransactionRepresentation.class ),
                any( TransactionApplicationMode.class ) ) ).thenReturn( ValidatedIndexUpdates.NONE );
        final TransactionRepresentationStoreApplier applier = mock(TransactionRepresentationStoreApplier.class);
        final TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, logRotation,
                checkPointer, transactionCommittedConsumer, transactionMetadataCache, transactionIdStore,
                IdOrderingQueue.BYPASS, health ) );
        life.start();


        TransactionCommittingResponseUnpacker.Dependencies deps =
                buildDependencies( transactionIdStore, logFile, health, logRotation,
                        indexUpdatesValidator,
                        applier, appender, mock( TransactionObligationFulfiller.class ) );

        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps );
        unpacker.start();

        // and a closed logFile/appender
        life.shutdown();

        // WHEN packing up a transaction response
        try
        {
            unpacker.unpackResponse( new DummyTransactionResponse( BASE_TX_ID + 1, 1, appender, 5 ), NO_OP_TX_HANDLER );
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            // THEN apart from failing we don't want any committed/closed calls to TransactionIdStore
            verify( transactionIdStore, times( 0 ) ).transactionCommitted( anyLong(), anyLong(), anyLong(), anyLong() );
            verify( transactionIdStore, times( 0 ) ).transactionClosed( anyLong() );
        }
    }

    private TransactionCommittingResponseUnpacker.Dependencies buildDependencies(
            final TransactionIdStore transactionIdStore, final LogFile logFile, final KernelHealth health,
            final LogRotation logRotation, final IndexUpdatesValidator indexUpdatesValidator,
            final TransactionRepresentationStoreApplier storeApplier,
            final TransactionAppender appender, final TransactionObligationFulfiller obligationFulfiller )
    {
        return new TransactionCommittingResponseUnpacker.Dependencies()
            {

            @Override
                public TransactionRepresentationStoreApplier transactionRepresentationStoreApplier()
                {
                    return storeApplier;
                }

                @Override
                public IndexUpdatesValidator indexUpdatesValidator()
                {
                    return indexUpdatesValidator;
                }

                @Override
                public TransactionIdStore transactionIdStore()
                {
                    return transactionIdStore;
                }

                @Override
                public Supplier<TransactionObligationFulfiller> transactionObligationFulfiller()
                {
                    return Suppliers.singleton( obligationFulfiller );
                }

                @Override
                public Supplier<TransactionAppender> transactionAppender()
                {
                    return Suppliers.singleton( appender );
                }

                @Override
                public LogFile logFile()
                {
                    return logFile;
                }

                @Override
                public LogRotation logRotation()
                {
                    return logRotation;
                }

                @Override
                public KernelHealth kernelHealth()
                {
                    return health;
                }
            };
    }

    private TransactionAppender mockedTransactionAppender() throws IOException
    {
        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ), anyLong() ) ).thenReturn( mock( Commitment
                .class ) );
        return appender;
    }

    private IndexUpdatesValidator setUpIndexUpdatesValidatorMocking( ) throws IOException
    {
        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );

        doReturn( ValidatedIndexUpdates.NONE )
                .when( indexUpdatesValidator )
                .validate( any( TransactionRepresentation.class ), any( TransactionApplicationMode.class ) );

        return indexUpdatesValidator;
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

        public DummyTransactionResponse( long startingAtTxId, int txCount, TransactionAppender appender, int
                maxBatchSize )
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
            when( mockStartEntry.checksum() ).thenReturn( id * 10 );
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
                handler.transactions().visit( tx( startingAtTxId + i ) );
                if ( (i + 1) % maxBatchSize == 0 )
                {
                    try
                    {
                        verify( appender, times( maxBatchSize ) ).append( any( TransactionRepresentation.class ),
                                anyLong() );
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
}
