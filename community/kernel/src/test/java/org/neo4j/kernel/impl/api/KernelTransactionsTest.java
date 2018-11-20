/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.security.AnonymousContext.none;
import static org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory.DEFAULT;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;
import static org.neo4j.test.MockedNeoStores.mockedTokenHolders;
import static org.neo4j.test.assertion.Assert.assertException;
import static org.neo4j.util.concurrent.Futures.combine;

public class KernelTransactionsTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2-" + getClass().getName() );
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static final long TEST_TIMEOUT = 10_000;
    private static final SystemNanoClock clock = Clocks.nanoClock();
    private static DatabaseAvailabilityGuard databaseAvailabilityGuard;

    @Before
    public void setUp()
    {
        databaseAvailabilityGuard = new DatabaseAvailabilityGuard( DEFAULT_DATABASE_NAME, clock, NullLog.getInstance() );
    }

    @Test
    public void shouldListActiveTransactions() throws Throwable
    {
        // Given
        KernelTransactions transactions = newTestKernelTransactions();

        // When
        KernelTransaction first = getKernelTransaction( transactions );
        KernelTransaction second = getKernelTransaction( transactions );
        KernelTransaction third = getKernelTransaction( transactions );

        first.close();

        // Then
        assertThat( transactions.activeTransactions(), equalTo( asSet( newHandle( second ), newHandle( third ) ) ) );
    }

    @Test
    public void shouldDisposeTransactionsWhenAsked() throws Throwable
    {
        // Given
        KernelTransactions transactions = newKernelTransactions();

        transactions.disposeAll();

        KernelTransaction first = getKernelTransaction( transactions );
        KernelTransaction second = getKernelTransaction( transactions );
        KernelTransaction leftOpen = getKernelTransaction( transactions );
        first.close();
        second.close();

        // When
        transactions.disposeAll();

        // Then
        KernelTransaction postDispose = getKernelTransaction( transactions );
        assertThat( postDispose, not( equalTo( first ) ) );
        assertThat( postDispose, not( equalTo( second ) ) );

        assertNotNull( leftOpen.getReasonIfTerminated() );
    }

    @Test
    public void shouldIncludeRandomBytesInAdditionalHeader() throws Throwable
    {
        // Given
        TransactionRepresentation[] transactionRepresentation = new TransactionRepresentation[1];

        KernelTransactions registry = newKernelTransactions( newRememberingCommitProcess( transactionRepresentation ) );

        // When
        try ( KernelTransaction transaction = getKernelTransaction( registry ) )
        {
            // Just pick anything that can flag that changes have been made to this transaction
            ((KernelTransactionImplementation) transaction).txState().nodeDoCreate( 0 );
            transaction.success();
        }

        // Then
        byte[] additionalHeader = transactionRepresentation[0].additionalHeader();
        assertNotNull( additionalHeader );
        assertTrue( additionalHeader.length > 0 );
    }

    @Test
    public void shouldReuseClosedTransactionObjects() throws Throwable
    {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = getKernelTransaction( transactions );

        // WHEN
        a.close();
        KernelTransaction b = getKernelTransaction( transactions );

        // THEN
        assertSame( a, b );
    }

    @Test
    public void shouldTellWhenTransactionsFromSnapshotHaveBeenClosed() throws Throwable
    {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = getKernelTransaction( transactions );
        KernelTransaction b = getKernelTransaction( transactions );
        KernelTransaction c = getKernelTransaction( transactions );
        KernelTransactionsSnapshot snapshot = transactions.get();
        assertFalse( snapshot.allClosed() );

        // WHEN a gets closed
        a.close();
        assertFalse( snapshot.allClosed() );

        // WHEN c gets closed and (test knowing too much) that instance getting reused in another transaction "d".
        c.close();
        KernelTransaction d = getKernelTransaction( transactions );
        assertFalse( snapshot.allClosed() );

        // WHEN b finally gets closed
        b.close();
        assertTrue( snapshot.allClosed() );
    }

    @Test
    public void shouldBeAbleToSnapshotDuringHeavyLoad() throws Throwable
    {
        // GIVEN
        final KernelTransactions transactions = newKernelTransactions();
        Race race = new Race();
        final int threads = 50;
        final AtomicBoolean end = new AtomicBoolean();
        final AtomicReferenceArray<KernelTransactionsSnapshot> snapshots = new AtomicReferenceArray<>( threads );

        // Representing "transaction" threads
        for ( int i = 0; i < threads; i++ )
        {
            final int threadIndex = i;
            race.addContestant( () ->
            {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !end.get() )
                {
                    try ( KernelTransaction ignored = getKernelTransaction( transactions ) )
                    {
                        parkNanos( MILLISECONDS.toNanos( random.nextInt( 3 ) ) );
                        if ( snapshots.get( threadIndex ) == null )
                        {
                            snapshots.set( threadIndex, transactions.get() );
                            parkNanos( MILLISECONDS.toNanos( random.nextInt( 3 ) ) );
                        }
                    }
                    catch ( TransactionFailureException e )
                    {
                        throw new RuntimeException( e );
                    }
                }
            } );
        }

        // Just checks snapshots
        race.addContestant( () ->
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            int snapshotsLeft = 1_000;
            while ( snapshotsLeft > 0 )
            {
                int threadIndex = random.nextInt( threads );
                KernelTransactionsSnapshot snapshot = snapshots.get( threadIndex );
                if ( snapshot != null && snapshot.allClosed() )
                {
                    snapshotsLeft--;
                    snapshots.set( threadIndex, null );
                }
            }

            // End condition of this test can be described as:
            //   when 1000 snapshots have been seen as closed.
            // setting this boolean to true will have all other threads end as well so that race.go() will end
            end.set( true );
        } );

        // WHEN
        race.go();
    }

    @Test
    public void transactionCloseRemovesTxFromActiveTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newTestKernelTransactions();

        KernelTransaction tx1 = getKernelTransaction( kernelTransactions );
        KernelTransaction tx2 = getKernelTransaction( kernelTransactions );
        KernelTransaction tx3 = getKernelTransaction( kernelTransactions );

        tx1.close();
        tx3.close();

        assertEquals( asSet( newHandle( tx2 ) ), kernelTransactions.activeTransactions() );
    }

    @Test
    public void disposeAllMarksAllTransactionsForTermination() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = getKernelTransaction( kernelTransactions );
        KernelTransaction tx2 = getKernelTransaction( kernelTransactions );
        KernelTransaction tx3 = getKernelTransaction( kernelTransactions );

        kernelTransactions.disposeAll();

        assertEquals( Status.General.DatabaseUnavailable, tx1.getReasonIfTerminated().get() );
        assertEquals( Status.General.DatabaseUnavailable, tx2.getReasonIfTerminated().get() );
        assertEquals( Status.General.DatabaseUnavailable, tx3.getReasonIfTerminated().get() );
    }

    @Test
    public void transactionClosesUnderlyingStoreReaderWhenDisposed() throws Throwable
    {
        StorageReader storeStatement1 = mock( StorageReader.class );
        StorageReader storeStatement2 = mock( StorageReader.class );
        StorageReader storeStatement3 = mock( StorageReader.class );

        KernelTransactions kernelTransactions = newKernelTransactions( mock( TransactionCommitProcess.class ),
                storeStatement1, storeStatement2, storeStatement3 );

        // start and close 3 transactions from different threads
        startAndCloseTransaction( kernelTransactions );
        Executors.newSingleThreadExecutor().submit( () -> startAndCloseTransaction( kernelTransactions ) ).get();
        Executors.newSingleThreadExecutor().submit( () -> startAndCloseTransaction( kernelTransactions ) ).get();

        kernelTransactions.disposeAll();

        verify( storeStatement1 ).close();
        verify( storeStatement2 ).close();
        verify( storeStatement3 ).close();
    }

    @Test
    public void threadThatBlocksNewTxsCantStartNewTxs() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();
        try
        {
            kernelTransactions.newInstance( KernelTransaction.Type.implicit, AnonymousContext.write(), EMBEDDED_CONNECTION, 0L );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test( timeout = TEST_TIMEOUT )
    public void blockNewTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener =
                t2.execute( state -> kernelTransactions.newInstance( explicit, AnonymousContext.write(), EMBEDDED_CONNECTION, 0L ) );
        t2.get().waitUntilWaiting( location -> location.isAt( KernelTransactions.class, "newInstance" ) );

        assertNotDone( txOpener );

        kernelTransactions.unblockNewTransactions();
        assertNotNull( txOpener.get() );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void unblockNewTransactionsFromWrongThreadThrows() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener =
                t2.execute( state -> kernelTransactions.newInstance( explicit, AnonymousContext.write(), EMBEDDED_CONNECTION, 0L ) );
        t2.get().waitUntilWaiting( location -> location.isAt( KernelTransactions.class, "newInstance" ) );

        assertNotDone( txOpener );

        Future<?> wrongUnblocker = unblockTxsInSeparateThread( kernelTransactions );

        try
        {
            wrongUnblocker.get();
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( IllegalStateException.class ) );
        }
        assertNotDone( txOpener );

        kernelTransactions.unblockNewTransactions();
        assertNotNull( txOpener.get() );
    }

    @Test
    public void shouldNotLeakTransactionOnSecurityContextFreezeFailure() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        LoginContext loginContext = mock( LoginContext.class );
        when( loginContext.authorize( any(), any() ) ).thenThrow( new AuthorizationExpiredException( "Freeze failed." ) );

        assertException( () -> kernelTransactions.newInstance( KernelTransaction.Type.explicit, loginContext, EMBEDDED_CONNECTION, 0L ),
                AuthorizationExpiredException.class, "Freeze failed.");

        assertThat("We should not have any transaction", kernelTransactions.activeTransactions(), is(empty()));
    }

    @Test
    public void exceptionWhenStartingNewTransactionOnShutdownInstance() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        databaseAvailabilityGuard.shutdown();

        expectedException.expect( DatabaseShutdownException.class );
        kernelTransactions.newInstance( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 0L );
    }

    @Test
    public void exceptionWhenStartingNewTransactionOnStoppedKernelTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        t2.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state ->
        {
            stopKernelTransactions( kernelTransactions );
            return null;
        } ).get();

        expectedException.expect( IllegalStateException.class );
        kernelTransactions.newInstance( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 0L );
    }

    @Test
    public void startNewTransactionOnRestartedKErnelTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        kernelTransactions.stop();
        kernelTransactions.start();
        assertNotNull( "New transaction created by restarted kernel transactions component.",
                kernelTransactions.newInstance( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 0L ) );
    }

    @Test
    public void incrementalUserTransactionId() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        try ( KernelTransaction kernelTransaction = kernelTransactions
                .newInstance( KernelTransaction.Type.explicit, AnonymousContext.none(), EMBEDDED_CONNECTION, 0L ) )
        {
            assertEquals( 1, kernelTransactions.activeTransactions().iterator().next().getUserTransactionId() );
        }

        try ( KernelTransaction kernelTransaction = kernelTransactions
                .newInstance( KernelTransaction.Type.explicit, AnonymousContext.none(), EMBEDDED_CONNECTION, 0L ) )
        {
            assertEquals( 2, kernelTransactions.activeTransactions().iterator().next().getUserTransactionId() );
        }

        try ( KernelTransaction kernelTransaction = kernelTransactions
                .newInstance( KernelTransaction.Type.explicit, AnonymousContext.none(), EMBEDDED_CONNECTION, 0L ) )
        {
            assertEquals( 3, kernelTransactions.activeTransactions().iterator().next().getUserTransactionId() );
        }
    }

    @Test
    public void doNotAllowToCreateMoreThenMaxActiveTransactions() throws Throwable
    {
        Config config = Config.defaults( GraphDatabaseSettings.max_concurrent_transactions, "2" );
        KernelTransactions kernelTransactions = newKernelTransactions( config );
        KernelTransaction ignore = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
        KernelTransaction ignore2 = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );

        expectedException.expect( MaximumTransactionLimitExceededException.class );
        kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
    }

    @Test
    public void allowToBeginTransactionsWhenSlotsAvailableAgain() throws Throwable
    {
        Config config = Config.defaults( GraphDatabaseSettings.max_concurrent_transactions, "2" );
        KernelTransactions kernelTransactions = newKernelTransactions( config );
        KernelTransaction ignore = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
        KernelTransaction ignore2 = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );

        try
        {
            kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
            fail( "should not be able to start" );
        }
        catch ( MaximumTransactionLimitExceededException e )
        {
            // expected
        }

        ignore.close();
        // fine to start again
        kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
    }

    @Test
    public void allowToBeginTransactionsWhenConfigChanges() throws Throwable
    {
        Config config = Config.defaults( GraphDatabaseSettings.max_concurrent_transactions, "2" );
        KernelTransactions kernelTransactions = newKernelTransactions( config );
        KernelTransaction ignore = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
        KernelTransaction ignore2 = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );

        try
        {
            kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
            fail( "should not be able to start" );
        }
        catch ( MaximumTransactionLimitExceededException e )
        {
            // expected
        }

        config.updateDynamicSetting( GraphDatabaseSettings.max_concurrent_transactions.name(), "3", "test" );

        // fine to start again
        kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L );
    }

    @Test
    public void reuseSameTransactionInOneThread() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        for ( int i  = 0; i < 100; i++ )
        {
            try ( KernelTransaction kernelTransaction = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L ) )
            {
                assertEquals( 1, kernelTransactions.getNumberOfActiveTransactions() );
            }
        }
        assertEquals( 0, kernelTransactions.getNumberOfActiveTransactions() );
    }

    @Test
    public void trackNumberOfActiveTransactionFromMultipleThreads() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        int expectedTransactions = 100;
        ExecutorService executors = Executors.newFixedThreadPool( expectedTransactions );
        Phaser phaser = new Phaser( expectedTransactions );
        List<Future<Void>> transactionFutures = new ArrayList<>();
        try
        {
            for ( int i = 0; i < expectedTransactions; i++ )
            {
                Future transactionFuture = executors.submit( () ->
                {
                    try ( KernelTransaction ignored = kernelTransactions.newInstance( explicit, none(), EMBEDDED_CONNECTION, 0L ) )
                    {
                        phaser.arriveAndAwaitAdvance();
                        assertEquals( expectedTransactions, kernelTransactions.getNumberOfActiveTransactions() );
                        phaser.arriveAndAwaitAdvance();
                    }
                    catch ( TransactionFailureException e )
                    {
                        throw new RuntimeException( e );
                    }
                    phaser.arriveAndDeregister();
                } );
                transactionFutures.add( transactionFuture );
            }
            combine( transactionFutures ).get();
            assertEquals( 0, kernelTransactions.getNumberOfActiveTransactions() );
        }
        finally
        {
            executors.shutdown();
        }
    }

    private static void stopKernelTransactions( KernelTransactions kernelTransactions )
    {
        try
        {
            kernelTransactions.stop();
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( t );
        }
    }

    private static void startAndCloseTransaction( KernelTransactions kernelTransactions )
    {
        try
        {
            kernelTransactions.newInstance( KernelTransaction.Type.explicit, AUTH_DISABLED, EMBEDDED_CONNECTION, 0L ).close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static KernelTransactions newKernelTransactions() throws Throwable
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ) );
    }

    private static KernelTransactions newKernelTransactions( Config config ) throws Throwable
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ), config );
    }

    private static KernelTransactions newTestKernelTransactions() throws Throwable
    {
        return newKernelTransactions( true, mock( TransactionCommitProcess.class ), mock( StorageReader.class ), Config.defaults() );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess, Config config ) throws Throwable
    {
        return newKernelTransactions( false, commitProcess, mock( StorageReader.class ), config );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess ) throws Throwable
    {
        return newKernelTransactions( false, commitProcess, mock( StorageReader.class ), Config.defaults() );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess,
            StorageReader firstReader, StorageReader... otherReaders ) throws Throwable
    {
        return newKernelTransactions( false, commitProcess, firstReader, Config.defaults(), otherReaders );
    }

    private static KernelTransactions newKernelTransactions( boolean testKernelTransactions, TransactionCommitProcess commitProcess, StorageReader firstReader,
            Config config, StorageReader... otherReaders ) throws Throwable
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        StorageEngine storageEngine = mock( StorageEngine.class );
        when( storageEngine.newReader() ).thenReturn( firstReader, otherReaders );
        when( storageEngine.newCommandCreationContext() ).thenReturn( mock( CommandCreationContext.class ) );
        doAnswer( invocation ->
        {
            Collection<StorageCommand> argument = invocation.getArgument( 0 );
            argument.add( mock( StorageCommand.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                anyCollection(),
                any( ReadableTransactionState.class ),
                any( StorageReader.class ),
                any( CommandCreationContext.class ),
                any( ResourceLocker.class ),
                anyLong(),
                any( TxStateVisitor.Decorator.class ) );

        return newKernelTransactions( locks, storageEngine, commitProcess, testKernelTransactions, config );
    }

    private static KernelTransactions newKernelTransactions( Locks locks, StorageEngine storageEngine, TransactionCommitProcess commitProcess,
            boolean testKernelTransactions, Config config )
    {
        LifeSupport life = new LifeSupport();
        life.start();

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 0, 0, 0 ) );

        Tracers tracers = new Tracers( "null", NullLog.getInstance(), new Monitors(), mock( JobScheduler.class ),
                clock );
        StatementLocksFactory statementLocksFactory = new SimpleStatementLocksFactory( locks );

        StatementOperationParts statementOperations = mock( StatementOperationParts.class );
        KernelTransactions transactions;
        if ( testKernelTransactions )
        {
            transactions = createTestTransactions( storageEngine, commitProcess, transactionIdStore, tracers,
                    statementLocksFactory, statementOperations, clock, databaseAvailabilityGuard );
        }
        else
        {
            transactions = createTransactions( storageEngine, commitProcess, transactionIdStore, tracers,
                    statementLocksFactory, statementOperations, clock, databaseAvailabilityGuard, config );
        }
        transactions.start();
        return transactions;
    }

    private static KernelTransactions createTransactions( StorageEngine storageEngine, TransactionCommitProcess commitProcess,
            TransactionIdStore transactionIdStore, Tracers tracers, StatementLocksFactory statementLocksFactory, StatementOperationParts statementOperations,
            SystemNanoClock clock, AvailabilityGuard databaseAvailabilityGuard, Config config )
    {
        return new KernelTransactions( config, statementLocksFactory, null, statementOperations,
                null, DEFAULT, commitProcess, new TransactionHooks(),
                mock( TransactionMonitor.class ), databaseAvailabilityGuard, tracers, storageEngine, new Procedures(), transactionIdStore, clock,
                new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ),
                new CanWrite(), EmptyVersionContextSupplier.EMPTY, ON_HEAP,
                mock( ConstraintSemantics.class ), mock( SchemaState.class ),
                mockedTokenHolders(), DEFAULT_DATABASE_NAME, mock( IndexingService.class ), mock( LabelScanStore.class ), new Dependencies() );
    }

    private static TestKernelTransactions createTestTransactions( StorageEngine storageEngine,
            TransactionCommitProcess commitProcess, TransactionIdStore transactionIdStore, Tracers tracers,
            StatementLocksFactory statementLocksFactory, StatementOperationParts statementOperations,
            SystemNanoClock clock, AvailabilityGuard databaseAvailabilityGuard )
    {
        return new TestKernelTransactions( statementLocksFactory, null, statementOperations, null, DEFAULT, commitProcess,
                new TransactionHooks(), mock( TransactionMonitor.class ), databaseAvailabilityGuard, tracers, storageEngine, new Procedures(),
                transactionIdStore, clock, new CanWrite(), EmptyVersionContextSupplier.EMPTY, mockedTokenHolders(), new Dependencies() );
    }

    private static TransactionCommitProcess newRememberingCommitProcess( final TransactionRepresentation[] slot )
            throws TransactionFailureException
    {
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        when( commitProcess.commit(
                any( TransactionToApply.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) ) )
                .then( invocation ->
                {
                    slot[0] = ((TransactionToApply) invocation.getArgument( 0 )).transactionRepresentation();
                    return 1L;
                } );

        return commitProcess;
    }

    private static Future<?> unblockTxsInSeparateThread( final KernelTransactions kernelTransactions )
    {
        return Executors.newSingleThreadExecutor().submit( kernelTransactions::unblockNewTransactions );
    }

    private static void assertNotDone( Future<?> future )
    {
        assertFalse( future.isDone() );
    }

    private static KernelTransactionHandle newHandle( KernelTransaction tx )
    {
        return new TestKernelTransactionHandle( tx );
    }

    private static KernelTransaction getKernelTransaction( KernelTransactions transactions )
    {
        return transactions.newInstance( KernelTransaction.Type.implicit, AnonymousContext.none(), EMBEDDED_CONNECTION, 0L );
    }

    private static class TestKernelTransactions extends KernelTransactions
    {
        TestKernelTransactions( StatementLocksFactory statementLocksFactory,
                ConstraintIndexCreator constraintIndexCreator, StatementOperationParts statementOperations,
                SchemaWriteGuard schemaWriteGuard, TransactionHeaderInformationFactory txHeaderFactory,
                TransactionCommitProcess transactionCommitProcess,
                TransactionHooks hooks, TransactionMonitor transactionMonitor, AvailabilityGuard databaseAvailabilityGuard, Tracers tracers,
                StorageEngine storageEngine, Procedures procedures, TransactionIdStore transactionIdStore, SystemNanoClock clock,
                AccessCapability accessCapability,
                VersionContextSupplier versionContextSupplier, TokenHolders tokenHolders, Dependencies dataSourceDependencies )
        {
            super( Config.defaults(), statementLocksFactory, constraintIndexCreator, statementOperations, schemaWriteGuard, txHeaderFactory,
                    transactionCommitProcess, hooks, transactionMonitor, databaseAvailabilityGuard, tracers,
                    storageEngine, procedures, transactionIdStore, clock, new AtomicReference<>( CpuClock.NOT_AVAILABLE ),
                    new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ), accessCapability,
                    versionContextSupplier, ON_HEAP, new StandardConstraintSemantics(), mock( SchemaState.class ), tokenHolders,
                    DEFAULT_DATABASE_NAME, mock( IndexingService.class ), mock( LabelScanStore.class ), dataSourceDependencies );
        }

        @Override
        KernelTransactionHandle createHandle( KernelTransactionImplementation tx )
        {
            return new TestKernelTransactionHandle( tx );
        }
    }
}
