/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.index.IndexConfigStore;
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
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageStatement;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory.DEFAULT;

public class KernelTransactionsTest
{
    @Test
    public void shouldListActiveTransactions() throws Exception
    {
        // Given
        KernelTransactions registry = newTestKernelTransactions();

        // When
        KernelTransaction first = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction second = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction third = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );

        first.close();

        // Then
        assertThat( registry.activeTransactions(), equalTo( asSet( newHandle( second ), newHandle( third ) ) ) );
    }

    @Test
    public void shouldDisposeTransactionsWhenAsked() throws Exception
    {
        // Given
        KernelTransactions registry = newKernelTransactions();

        registry.disposeAll();

        KernelTransaction first = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction second = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction leftOpen = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        first.close();
        second.close();

        // When
        registry.disposeAll();

        // Then
        KernelTransaction postDispose = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        assertThat( postDispose, not( equalTo( first ) ) );
        assertThat( postDispose, not( equalTo( second ) ) );

        assertTrue( leftOpen.getReasonIfTerminated() != null );
    }

    @Test
    public void shouldIncludeRandomBytesInAdditionalHeader() throws Exception
    {
        // Given
        TransactionRepresentation[] transactionRepresentation = new TransactionRepresentation[1];

        KernelTransactions registry = newKernelTransactions( newRememberingCommitProcess( transactionRepresentation ) );

        // When
        try ( KernelTransaction transaction = registry.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE ) )
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
    public void shouldReuseClosedTransactionObjects() throws Exception
    {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );

        // WHEN
        a.close();
        KernelTransaction b = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );

        // THEN
        assertSame( a, b );
    }

    @Test
    public void shouldTellWhenTransactionsFromSnapshotHaveBeenClosed() throws Exception
    {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction b = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction c = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransactionsSnapshot snapshot = transactions.get();
        assertFalse( snapshot.allClosed() );

        // WHEN a gets closed
        a.close();
        assertFalse( snapshot.allClosed() );

        // WHEN c gets closed and (test knowing too much) that instance getting reused in another transaction "d".
        c.close();
        KernelTransaction d = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
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
            race.addContestant( () -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while ( !end.get() )
                {
                    try ( KernelTransaction transaction = transactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE ) )
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
        race.addContestant( () -> {
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
    public void transactionCloseRemovesTxFromActiveTransactions() throws Exception
    {
        KernelTransactions kernelTransactions = newTestKernelTransactions();

        KernelTransaction tx1 = kernelTransactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction tx2 = kernelTransactions.newInstance( KernelTransaction.Type.implicit,AccessMode.Static.NONE );
        KernelTransaction tx3 = kernelTransactions.newInstance( KernelTransaction.Type.implicit,AccessMode.Static.NONE );

        tx1.close();
        tx3.close();

        assertEquals( asSet( newHandle( tx2 ) ), kernelTransactions.activeTransactions() );
    }

    @Test
    public void disposeAllMarksAllTransactionsForTermination() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = kernelTransactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction tx2 = kernelTransactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );
        KernelTransaction tx3 = kernelTransactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.NONE );

        kernelTransactions.disposeAll();

        assertEquals( Status.General.DatabaseUnavailable, tx1.getReasonIfTerminated() );
        assertEquals( Status.General.DatabaseUnavailable, tx2.getReasonIfTerminated() );
        assertEquals( Status.General.DatabaseUnavailable, tx3.getReasonIfTerminated() );
    }

    @Test
    public void transactionClosesUnderlyingStoreStatementWhenDisposed() throws Exception
    {
        StorageStatement storeStatement1 = mock( StorageStatement.class );
        StorageStatement storeStatement2 = mock( StorageStatement.class );
        StorageStatement storeStatement3 = mock( StorageStatement.class );

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
    public void threadThatBlocksNewTxsCantStartNewTxs() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();
        try
        {
            kernelTransactions.newInstance( KernelTransaction.Type.implicit, AccessMode.Static.WRITE );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void blockNewTransactions() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        CountDownLatch aboutToStartTx = new CountDownLatch( 1 );
        Future<KernelTransaction> txOpener = startTxInSeparateThread( kernelTransactions, aboutToStartTx );

        await( aboutToStartTx );
        assertNotDone( txOpener );

        kernelTransactions.unblockNewTransactions();
        assertNotNull( txOpener.get( 2, TimeUnit.SECONDS ) );
    }

    @Test
    public void unblockNewTransactionsFromWrongThreadThrows() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        CountDownLatch aboutToStartTx = new CountDownLatch( 1 );
        Future<KernelTransaction> txOpener = startTxInSeparateThread( kernelTransactions, aboutToStartTx );

        await( aboutToStartTx );
        assertNotDone( txOpener );

        Future<?> wrongUnblocker = unblockTxsInSeparateThread( kernelTransactions );

        try
        {
            wrongUnblocker.get( 2, TimeUnit.SECONDS );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ExecutionException.class ) );
            assertThat( e.getCause(), instanceOf( IllegalStateException.class ) );
        }
        assertNotDone( txOpener );

        kernelTransactions.unblockNewTransactions();
        assertNotNull( txOpener.get( 2, TimeUnit.SECONDS ) );
    }

    private static void startAndCloseTransaction( KernelTransactions kernelTransactions )
    {
        try
        {
            kernelTransactions.newInstance( KernelTransaction.Type.explicit, AccessMode.Static.FULL ).close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static KernelTransactions newKernelTransactions() throws Exception
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ) );
    }

    private static KernelTransactions newTestKernelTransactions() throws Exception
    {
        return newKernelTransactions( true, mock( TransactionCommitProcess.class ), mock( StorageStatement.class ) );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess ) throws Exception
    {
        return newKernelTransactions( false, commitProcess, mock( StorageStatement.class ) );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess,
            StorageStatement firstStoreStatements, StorageStatement... otherStorageStatements ) throws Exception
    {
        return newKernelTransactions( false, commitProcess, firstStoreStatements, otherStorageStatements );
    }

    private static KernelTransactions newKernelTransactions( boolean testKernelTransactions,
            TransactionCommitProcess commitProcess, StorageStatement firstStoreStatements,
            StorageStatement... otherStorageStatements ) throws Exception
    {
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        StoreReadLayer readLayer = mock( StoreReadLayer.class );
        when( readLayer.newStatement() ).thenReturn( firstStoreStatements, otherStorageStatements );

        StorageEngine storageEngine = mock( StorageEngine.class );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
        doAnswer( invocation ->
        {
            invocation.getArgumentAt( 0, Collection.class ).add( mock( StorageCommand.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                anyCollection(),
                any( ReadableTransactionState.class ),
                any( StorageStatement.class ),
                any( ResourceLocker.class ),
                anyLong() );

        return newKernelTransactions( locks, storageEngine, commitProcess, testKernelTransactions );
    }

    private static KernelTransactions newKernelTransactions( Locks locks, StorageEngine storageEngine,
            TransactionCommitProcess commitProcess, boolean testKernelTransactions )
    {
        LifeSupport life = new LifeSupport();
        life.start();

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 0, 0, 0 ) );

        Tracers tracers = new Tracers( "null", NullLog.getInstance(), new Monitors(), mock( JobScheduler.class ) );
        StatementLocksFactory statementLocksFactory = new SimpleStatementLocksFactory( locks );

        if ( testKernelTransactions )
        {
            return new TestKernelTransactions( statementLocksFactory, null, null, null,
                    DEFAULT,
                    commitProcess, null, null, new TransactionHooks(), mock( TransactionMonitor.class ), life,
                    tracers, storageEngine, new Procedures(), transactionIdStore, Clocks.systemClock() );
        }
        return new KernelTransactions( statementLocksFactory,
                null, null, null, DEFAULT,
                commitProcess, null, null, new TransactionHooks(), mock( TransactionMonitor.class ), life,
                tracers, storageEngine, new Procedures(), transactionIdStore, Clocks.systemClock() );
    }

    private static TransactionCommitProcess newRememberingCommitProcess( final TransactionRepresentation[] slot )
            throws TransactionFailureException
    {
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        when( commitProcess.commit(
                any( TransactionToApply.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) ) )
                .then( invocation -> {
                    slot[0] = ((TransactionToApply) invocation.getArguments()[0]).transactionRepresentation();
                    return 1L;
                } );

        return commitProcess;
    }

    private static Future<KernelTransaction> startTxInSeparateThread( final KernelTransactions kernelTransactions,
            final CountDownLatch aboutToStartTx )
    {
        return Executors.newSingleThreadExecutor().submit( new Callable<KernelTransaction>()
        {
            @Override
            public KernelTransaction call()
            {
                aboutToStartTx.countDown();
                return kernelTransactions.newInstance( KernelTransaction.Type.explicit, AccessMode.Static.WRITE );
            }
        } );
    }

    private static Future<?> unblockTxsInSeparateThread( final KernelTransactions kernelTransactions )
    {
        return Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                kernelTransactions.unblockNewTransactions();
            }
        } );
    }

    private void await( CountDownLatch latch ) throws InterruptedException
    {
        assertTrue( latch.await( 1, MINUTES ) );
    }

    private static void assertNotDone( Future<?> future )
    {
        try
        {
            future.get( 2, TimeUnit.SECONDS );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( TimeoutException.class ) );
        }
    }

    private static KernelTransactionHandle newHandle( KernelTransaction tx )
    {
        return new TestKernelTransactionHandle( tx );
    }

    private static class TestKernelTransactions extends KernelTransactions
    {
        public TestKernelTransactions( StatementLocksFactory statementLocksFactory,
                ConstraintIndexCreator constraintIndexCreator,
                StatementOperationParts statementOperations, SchemaWriteGuard schemaWriteGuard,
                TransactionHeaderInformationFactory txHeaderFactory,
                TransactionCommitProcess transactionCommitProcess,
                IndexConfigStore indexConfigStore,
                LegacyIndexProviderLookup legacyIndexProviderLookup, TransactionHooks hooks,
                TransactionMonitor transactionMonitor, LifeSupport dataSourceLife,
                Tracers tracers, StorageEngine storageEngine, Procedures procedures,
                TransactionIdStore transactionIdStore, Clock clock )
        {
            super( statementLocksFactory, constraintIndexCreator, statementOperations, schemaWriteGuard, txHeaderFactory,
                    transactionCommitProcess, indexConfigStore, legacyIndexProviderLookup, hooks, transactionMonitor,
                    dataSourceLife, tracers, storageEngine, procedures, transactionIdStore, clock );
        }

        @Override
        KernelTransactionHandle createHandle( KernelTransactionImplementation tx )
        {
            return new TestKernelTransactionHandle( tx );
        }
    }
}
