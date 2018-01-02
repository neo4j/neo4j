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
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.ResourceReleaser;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.com.TransactionStream;
import org.neo4j.com.TransactionStreamResponse;
import org.neo4j.com.storecopy.TransactionCommittingResponseUnpacker.Dependencies;
import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.api.BatchingTransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.index.IndexUpdatesValidator;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.BatchingTransactionAppender;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.LogFile;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    @Rule
    public final LifeRule life = new LifeRule();

    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private final LogService logging = new SimpleLogService(
            new AssertableLogProvider(), new AssertableLogProvider() );


    @Test
    public void panicAndSkipBatchOnApplyingFailure() throws Throwable
    {
        final long committingTransactionId = BASE_TX_ID + 1;
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        BatchingTransactionRepresentationStoreApplier applier = mock( BatchingTransactionRepresentationStoreApplier.class );
        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ), anyLong() ) )
                .thenAnswer( new FakeCommitmentAnswer( committingTransactionId, txIdStore ) );

        LogFile logFile = mock( LogFile.class );
        LogRotation logRotation = mock( LogRotation.class );
        KernelHealth kernelHealth = newKernelHealth();

        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        String errorMessage = "Too many open files";

        doReturn(null).
        doThrow( new IOException( errorMessage ) )
                .when( indexUpdatesValidator )
                .validate( any( TransactionRepresentation.class ) );

        TransactionCommittingResponseUnpacker.Dependencies dependencies = buildDependencies( logFile,
                logRotation, indexUpdatesValidator, applier, appender,
                mock( TransactionObligationFulfiller.class ), kernelHealth );

        int maxBatchSize = 10;
        DummyTransactionResponse response = new DummyTransactionResponse( committingTransactionId, 10, appender,
                maxBatchSize );

        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( dependencies, maxBatchSize );
        unpacker.start();

        try
        {
            unpacker.unpackResponse( response, NO_OP_TX_HANDLER );
            fail("Should fail during batch processing");
        }
        catch ( IOException ignore )
        {
            // ignored
        }

        assertFalse("Kernel should be unhealthy because of failure during index updates validation.", kernelHealth.isHealthy() );
        assertEquals( "Root cause should have expected exception",
                errorMessage, kernelHealth.getCauseOfPanic().getMessage() );

        // 2 transactions where committed by none was closed.
        verify( txIdStore, times( 2 ) ).transactionCommitted( anyLong(), anyLong(), anyLong() );
        verify( txIdStore, times( 2 ) ).transactionClosed( anyLong(), anyLong(), anyLong() );
    }

    /*
     * Tests that we unfreeze active transactions after commit and after apply of batch if batch length (in time)
     * is larger than safeZone time.
     */
    @Test
    public void shouldUnfreezeKernelTransactionsAfterApplyIfBatchIsLarge() throws Throwable
    {
        // GIVEN
        final TransactionAppender appender = mockedTransactionAppender();
        final IndexUpdatesValidator indexUpdatesValidator = mockedIndexUpdatesValidator(  );
        final KernelHealth kernelHealth = newKernelHealth();
        final long idReuseSafeZoneTime = 100;

        Dependencies deps = new MockedDependencies()
                .indexUpdatesValidator( indexUpdatesValidator )
                .kernelHealth( kernelHealth )
                .transactionAppender( appender )
                .idReuseSafeZoneTime( idReuseSafeZoneTime );

        int maxBatchSize = 3;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps, maxBatchSize );
        unpacker.start();

        // WHEN
        int txCount = maxBatchSize;
        int doesNotMatter = 1;
        unpacker.unpackResponse(
                new DummyTransactionResponse( doesNotMatter, txCount, appender, maxBatchSize, idReuseSafeZoneTime + 1 ),
                NO_OP_TX_HANDLER );

        // THEN
        KernelTransactions kernelTransactions = deps.kernelTransactions();
        BatchingTransactionRepresentationStoreApplier applier =
                deps.transactionRepresentationStoreApplier();
        InOrder inOrder = inOrder( kernelTransactions, applier );
        inOrder.verify( applier, times( 1 ) ).closeBatch();
        inOrder.verify( kernelTransactions, times( 1 ) ).unblockNewTransactions();
    }

    /*
     * Tests that shutting down the response unpacker while in the middle of committing a transaction will
     * allow that transaction stream to complete committing. It also verifies that any subsequent transactions
     * won't begin the commit process at all.
     * @throws Throwable
     */
    @Test
    public void testStopShouldAllowTransactionsToCompleteCommitAndApply() throws Throwable
    {
        // Given
        long committingTransactionId = BASE_TX_ID + 1;

        // Handcrafted deep mocks, otherwise the dependency resolution throws ClassCastExceptions
        final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
        final TransactionAppender appender = mockedTransactionAppender();
        final LogRotation logRotation = mock( LogRotation.class );
        final IndexUpdatesValidator indexUpdatesValidator = mockedIndexUpdatesValidator();
        final KernelHealth kernelHealth = newKernelHealth();

          /*
           * The tx handler is called on every transaction applied after setting its id to committing
           * but before setting it to applied. We use this to stop the unpacker in the middle of the
           * process.
           */
        StoppingTxHandler stoppingTxHandler = new StoppingTxHandler();

        Dependencies deps = new MockedDependencies()
                .transactionAppender( appender )
                .indexUpdatesValidator( indexUpdatesValidator )
                .kernelHealth( kernelHealth )
                .logRotation( logRotation );
        when( appender.append( any( TransactionRepresentation.class ), eq( committingTransactionId ) ) )
                .thenReturn( new FakeCommitment( committingTransactionId, txIdStore ) );

        int maxBatchSize = 10;
        TransactionCommittingResponseUnpacker unpacker =
                new TransactionCommittingResponseUnpacker( deps, maxBatchSize );
        stoppingTxHandler.setUnpacker( unpacker );

        // When
        unpacker.start();
        DummyTransactionResponse response = new DummyTransactionResponse( committingTransactionId, 1, appender,
                maxBatchSize );
        unpacker.unpackResponse( response, stoppingTxHandler );

        // Then
        verify( txIdStore, times( 1 ) ).transactionCommitted( eq( committingTransactionId ), anyLong(), anyLong() );
        verify( txIdStore, times( 1 ) ).transactionClosed( eq( committingTransactionId ), anyLong(), anyLong() );
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
        final BatchingTransactionRepresentationStoreApplier applier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        final TransactionAppender appender = mockedTransactionAppender();
        final IndexUpdatesValidator indexUpdatesValidator = mockedIndexUpdatesValidator(  );
        final LogFile logFile = mock( LogFile.class );
        final LogRotation logRotation = mock( LogRotation.class );
        final KernelHealth kernelHealth = newKernelHealth();

        Dependencies deps = new MockedDependencies()
                .logFile( logFile )
                .logRotation( logRotation )
                .indexUpdatesValidator( indexUpdatesValidator )
                .transactionRepresentationStoreApplier( applier )
                .transactionAppender( appender )
                .kernelHealth( kernelHealth );

        int maxBatchSize = 3;
        TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps, maxBatchSize );

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
        final TransactionAppender appender = mock( TransactionAppender.class );
        final BatchingTransactionRepresentationStoreApplier applier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        final TransactionObligationFulfiller fulfiller = mock( TransactionObligationFulfiller.class );

        Dependencies deps = new MockedDependencies()
                .transactionRepresentationStoreApplier( applier )
                .transactionAppender( appender )
                .transactionObligationFulfiller( fulfiller );

        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps );
        unpacker.start();

        // WHEN
        unpacker.unpackResponse( new DummyObligationResponse( 4 ), NO_OP_TX_HANDLER );

        // THEN
        verify( fulfiller, times( 1 ) ).fulfill( 4l );
    }

    @Test
    public void shouldThrowInCaseOfFailureToAppend() throws Throwable
    {
        // GIVEN
        final TransactionAppender appender = mock( TransactionAppender.class );
        final BatchingTransactionRepresentationStoreApplier applier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        final TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        final LogFile logFile = mock( LogFile.class );
        final LogRotation logRotation = mock( LogRotation.class );

        Dependencies deps = new MockedDependencies()
                .logFile( logFile )
                .logRotation( logRotation )
                .transactionRepresentationStoreApplier( applier )
                .transactionAppender( appender )
                .transactionObligationFulfiller( obligationFulfiller )
                .kernelHealth( newKernelHealth() );

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
        }
    }

    @Test
    public void shouldThrowInCaseOfFailureToApply() throws Throwable
    {
        // GIVEN
        TransactionIdStore txIdStore = mock( TransactionIdStore.class );

        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ), anyLong() ) ).thenReturn(
                new FakeCommitment( BASE_TX_ID + 1, txIdStore ) );

        TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        LogFile logFile = mock( LogFile.class );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        when( kernelHealth.isHealthy() ).thenReturn( true );
        LogRotation logRotation = mock( LogRotation.class );
        BatchingTransactionRepresentationStoreApplier applier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        Dependencies deps = new MockedDependencies()
                .logFile( logFile )
                .logRotation( logRotation )
                .transactionRepresentationStoreApplier( applier )
                .transactionAppender( appender )
                .transactionObligationFulfiller( obligationFulfiller )
                .kernelHealth( kernelHealth );
        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker(
                deps );
        unpacker.start();

        // WHEN failing to append one or more transactions from a transaction stream response
        UnderlyingStorageException failure = new UnderlyingStorageException( "Expected failure" );
        doThrow( failure ).when( applier ).apply( any( TransactionRepresentation.class ),
                any( ValidatedIndexUpdates.class ), any( LockGroup.class ), anyLong(),
                any( TransactionApplicationMode.class ) );
        try
        {
            unpacker.unpackResponse(
                    new DummyTransactionResponse( BASE_TX_ID + 1, 1, appender, 10 ), NO_OP_TX_HANDLER );
            fail( "Should have failed" );
        }
        catch ( UnderlyingStorageException e )
        {
            assertThat( e.getMessage(), containsString( failure.getMessage() ) );
        }
    }

    @Test
    public void shouldThrowIOExceptionIfKernelIsNotHealthy() throws Throwable
    {
        // GIVEN
        TransactionIdStore txIdStore = mock( TransactionIdStore.class );

        TransactionAppender appender = mock( TransactionAppender.class );
        when( appender.append( any( TransactionRepresentation.class ), anyLong() ) ).thenReturn(
                new FakeCommitment( BASE_TX_ID + 1, txIdStore ) );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TransactionObligationFulfiller obligationFulfiller = mock( TransactionObligationFulfiller.class );
        LogFile logFile = mock( LogFile.class );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        when( kernelHealth.isHealthy() ).thenReturn( false );
        Throwable causeOfPanic = new Throwable( "BOOM!" );
        when( kernelHealth.getCauseOfPanic() ).thenReturn( causeOfPanic );
        LogRotation logRotation = mock( LogRotation.class );
        Function<DependencyResolver,IndexUpdatesValidator> indexUpdatesValidatorFunction =
                Functions.constant( mock( IndexUpdatesValidator.class ) );
        BatchingTransactionRepresentationStoreApplier applier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        Function<DependencyResolver,BatchingTransactionRepresentationStoreApplier> transactionStoreApplierFunction =
                Functions.constant( applier );

        Dependencies deps = new MockedDependencies()
                .logFile( logFile )
                .logRotation( logRotation )
                .transactionRepresentationStoreApplier( applier )
                .transactionAppender( appender )
                .transactionObligationFulfiller( obligationFulfiller )
                .kernelHealth( kernelHealth );
        final TransactionCommittingResponseUnpacker unpacker = new TransactionCommittingResponseUnpacker( deps );
        unpacker.start();

        try
        {
            // WHEN failing to append one or more transactions from a transaction stream response
            unpacker.unpackResponse(
                    new DummyTransactionResponse( BASE_TX_ID + 1, 1, appender, 10 ), NO_OP_TX_HANDLER );
            fail( "should have thrown" );
        }
        catch ( IOException e )
        {
            assertEquals( TransactionCommittingResponseUnpacker.msg, e.getMessage() );
            assertEquals( causeOfPanic, e.getCause() );
            ((AssertableLogProvider)logging.getInternalLogProvider()).assertContainsMessageContaining(
                    TransactionCommittingResponseUnpacker.msg + " Original kernel panic cause was:\n" +
                            causeOfPanic.getMessage() );
        }
    }

    @Test
    public void shouldNotApplyTransactionIfIndexUpdatesValidationFails() throws Throwable
    {
        // Given
        final TransactionAppender appender = mockedTransactionAppender();
        final BatchingTransactionRepresentationStoreApplier storeApplier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        final IndexUpdatesValidator validator = mock( IndexUpdatesValidator.class );
        IOException error = new IOException( "error" );
        when( validator.validate( any( TransactionRepresentation.class ) ) ).thenThrow( error );

        Dependencies deps = new MockedDependencies()
                .indexUpdatesValidator( validator )
                .transactionRepresentationStoreApplier( storeApplier )
                .transactionAppender( appender )
                .kernelHealth( newKernelHealth() );

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
        final IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        when( indexUpdatesValidator.validate( any( TransactionRepresentation.class ) ) )
                .thenReturn( ValidatedIndexUpdates.NONE );
        final TransactionAppender appender = life.add( new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, IdOrderingQueue.BYPASS, health ) );
        life.start();

        Dependencies deps = new MockedDependencies()
                .logFile( logFile )
                .logRotation( logRotation )
                .indexUpdatesValidator( indexUpdatesValidator )
                .transactionAppender( appender )
                .kernelHealth( health );
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
            verify( transactionIdStore, times( 0 ) ).transactionCommitted( anyLong(), anyLong(), anyLong() );
            verify( transactionIdStore, times( 0 ) ).transactionClosed( anyLong(), anyLong(), anyLong() );
        }
    }

    private KernelHealth newKernelHealth()
    {
        Log log = logging.getInternalLog( getClass() );
        return new KernelHealth( new KernelPanicEventGenerator( new KernelEventHandlers( log ) ), log );
    }

    private class MockedDependencies implements Dependencies
    {
        BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier =
                mock( BatchingTransactionRepresentationStoreApplier.class );
        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );
        LogFile logFile = mock( LogFile.class );
        LogRotation logRotation = mock( LogRotation.class );
        KernelHealth kernelHealth = mock( KernelHealth.class );
        TransactionObligationFulfiller transactionObligationFulfiller = mock( TransactionObligationFulfiller.class );
        TransactionAppender transactionAppender = mock( TransactionAppender.class );
        KernelTransactions kernelTransactions = mock( KernelTransactions.class );
        long idReuseSafeZoneTime = 0;

        @Override
        public BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier()
        {
            return transactionRepresentationStoreApplier;
        }

        @Override
        public IndexUpdatesValidator indexUpdatesValidator()
        {
            return indexUpdatesValidator;
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
            return kernelHealth;
        }

        @Override
        public Supplier<TransactionObligationFulfiller> transactionObligationFulfiller()
        {
            return Suppliers.singleton( transactionObligationFulfiller );
        }

        @Override
        public Supplier<TransactionAppender> transactionAppender()
        {
            return Suppliers.singleton( transactionAppender );
        }

        @Override
        public KernelTransactions kernelTransactions()
        {
            return kernelTransactions;
        }

        @Override
        public LogService logService()
        {
            return logging;
        }

        @Override
        public long idReuseSafeZoneTime()
        {
            return idReuseSafeZoneTime;
        }

        public MockedDependencies transactionRepresentationStoreApplier(
                BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier )
        {
            this.transactionRepresentationStoreApplier = transactionRepresentationStoreApplier;
            return this;
        }

        public MockedDependencies indexUpdatesValidator(
                IndexUpdatesValidator indexUpdatesValidator )
        {
            this.indexUpdatesValidator = indexUpdatesValidator;
            return this;
        }

        public MockedDependencies logFile( LogFile logFile )
        {
            this.logFile = logFile;
            return this;
        }

        public MockedDependencies logRotation( LogRotation logRotation )
        {
            this.logRotation = logRotation;
            return this;
        }

        public MockedDependencies kernelHealth( KernelHealth kernelHealth )
        {
            this.kernelHealth = kernelHealth;
            return this;
        }

        public MockedDependencies transactionObligationFulfiller(
                TransactionObligationFulfiller transactionObligationFulfiller )
        {
            this.transactionObligationFulfiller = transactionObligationFulfiller;
            return this;
        }

        public MockedDependencies transactionAppender( TransactionAppender transactionAppender )
        {
            this.transactionAppender = transactionAppender;
            return this;
        }

        public MockedDependencies kernelTransactions( KernelTransactions kernelTransactions )
        {
            this.kernelTransactions = kernelTransactions;
            return this;
        }

        public MockedDependencies idReuseSafeZoneTime( long idReuseSafeZoneTime )
        {
            this.idReuseSafeZoneTime = idReuseSafeZoneTime;
            return this;
        }
    }

    private TransactionCommittingResponseUnpacker.Dependencies buildDependencies(
            final LogFile logFile,  final LogRotation logRotation,
            final IndexUpdatesValidator indexUpdatesValidator,
            final BatchingTransactionRepresentationStoreApplier storeApplier,
            final TransactionAppender appender, final TransactionObligationFulfiller obligationFulfiller,
            final KernelHealth kernelHealth )
    {
        return new TransactionCommittingResponseUnpacker.Dependencies()
        {
            @Override
            public BatchingTransactionRepresentationStoreApplier transactionRepresentationStoreApplier()
            {
                return storeApplier;
            }

            @Override
            public IndexUpdatesValidator indexUpdatesValidator()
            {
                return indexUpdatesValidator;
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
            public KernelTransactions kernelTransactions()
            {
                return mock( KernelTransactions.class );
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
                return kernelHealth;
            }

            @Override
            public LogService logService()
            {
                return logging;
            }

            @Override
            public long idReuseSafeZoneTime()
            {
                return 0;
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

    private IndexUpdatesValidator mockedIndexUpdatesValidator( ) throws IOException
    {
        IndexUpdatesValidator indexUpdatesValidator = mock( IndexUpdatesValidator.class );

        doReturn( ValidatedIndexUpdates.NONE )
                .when( indexUpdatesValidator )
                .validate( any( TransactionRepresentation.class ) );

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
        private final long batchLength;
        private static final long UNDEFINED_BATCH_LENGTH = -1;

        public DummyTransactionResponse( long startingAtTxId, int txCount, TransactionAppender appender, int
                maxBatchSize )
        {
            this( startingAtTxId, txCount, appender, maxBatchSize, UNDEFINED_BATCH_LENGTH );
        }

        public DummyTransactionResponse( long startingAtTxId, int txCount, TransactionAppender appender,
                int maxBatchSize, long batchLength )
        {
            super( new Object(), StoreId.DEFAULT, mock( TransactionStream.class ), ResourceReleaser.NO_OP );
            this.startingAtTxId = startingAtTxId;
            this.txCount = txCount;
            this.appender = appender;
            this.maxBatchSize = maxBatchSize;
            this.batchLength = batchLength;
        }

        private CommittedTransactionRepresentation tx( long id, long commitTimestamp )
        {
            CommittedTransactionRepresentation tx = mock( CommittedTransactionRepresentation.class );
            LogEntryCommit mockCommitEntry = mock( LogEntryCommit.class );
            when( mockCommitEntry.getTxId() ).thenReturn( id );
            when( mockCommitEntry.getTimeWritten() ).thenReturn( commitTimestamp );
            when( tx.getCommitEntry() ).thenReturn( mockCommitEntry );
            LogEntryStart mockStartEntry = mock( LogEntryStart.class );
            when( mockStartEntry.checksum() ).thenReturn( id * 10 );
            when( tx.getStartEntry() ).thenReturn( mockStartEntry );
            TransactionRepresentation txRepresentation = mock( TransactionRepresentation.class );
            when( txRepresentation.additionalHeader() ).thenReturn( new byte[0] );
            when( tx.getTransactionRepresentation() ).thenReturn( txRepresentation );
            return tx;
        }

        private long timestamp( int txNbr, int txCount, long batchLength )
        {
            if ( txCount == 1 )
            {
                return 0;
            }
            return txNbr * batchLength/( txCount-1 );
        }

        @Override
        public void accept( Response.Handler handler ) throws IOException
        {
            for ( int i = 0; i < txCount; i++ )
            {
                handler.transactions().visit( tx( startingAtTxId + i, timestamp( i, txCount, batchLength ) ) );
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

    private static class FakeCommitmentAnswer implements Answer<FakeCommitment>
    {
        private final long committingTransactionId;
        private final TransactionIdStore txIdStore;

        FakeCommitmentAnswer( long committingTransactionId, TransactionIdStore txIdStore )
        {
            this.committingTransactionId = committingTransactionId;
            this.txIdStore = txIdStore;
        }

        @Override
        public FakeCommitment answer( InvocationOnMock invocation ) throws Throwable
        {
            return new FakeCommitment( committingTransactionId, txIdStore );
        }
    }
}
