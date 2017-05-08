/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Clock;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.security.AuthorizationExpiredException;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.CanWrite;
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
import org.neo4j.storageengine.api.SchemaResources;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.api.KernelTransaction.Type.explicit;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory.DEFAULT;
import static org.neo4j.test.assertion.Assert.assertException;

public class KernelTransactionsTest
{
    @Rule
    public final OtherThreadRule<Void> t2 = new OtherThreadRule<>( "T2-" + getClass().getName() );
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private static final Clock clock = Clock.systemUTC();
    private static AvailabilityGuard availabilityGuard;

    @Before
    public void setUp()
    {
        availabilityGuard = new AvailabilityGuard( clock, NullLog.getInstance() );
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

        assertTrue( leftOpen.getReasonIfTerminated() != null );
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
            ((KernelTransactionImplementation) transaction).writableTxState().nodeDoCreate( 0 );
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
                    try ( KernelTransaction transaction = getKernelTransaction( transactions ) )
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
    public void transactionClosesUnderlyingStoreStatementWhenDisposed() throws Throwable
    {
        SchemaResources storeStatement1 = mock( SchemaResources.class );
        SchemaResources storeStatement2 = mock( SchemaResources.class );
        SchemaResources storeStatement3 = mock( SchemaResources.class );

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
            kernelTransactions.newInstance( KernelTransaction.Type.implicit, AnonymousContext.write(), 0L );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void blockNewTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener =
                t2.execute( state -> kernelTransactions.newInstance( explicit, AnonymousContext.write(), 0L ) );
        t2.get().waitUntilWaiting( location -> location.isAt( KernelTransactions.class, "newInstance" ) );

        assertNotDone( txOpener );

        kernelTransactions.unblockNewTransactions();
        assertNotNull( txOpener.get( 2, TimeUnit.SECONDS ) );
    }

    @Test
    public void unblockNewTransactionsFromWrongThreadThrows() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();

        Future<KernelTransaction> txOpener =
                t2.execute( state -> kernelTransactions.newInstance( explicit, AnonymousContext.write(), 0L ) );
        t2.get().waitUntilWaiting( location -> location.isAt( KernelTransactions.class, "newInstance" ) );

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

    @Test
    public void shouldNotLeakTransactionOnSecurityContextFreezeFailure() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        SecurityContext securityContext = mock(SecurityContext.class);
        when( securityContext.freeze() ).thenThrow( new AuthorizationExpiredException( "Freeze failed." ) );

        assertException(() -> kernelTransactions.newInstance(KernelTransaction.Type.explicit, securityContext, 0L),
                AuthorizationExpiredException.class, "Freeze failed.");

        assertThat("We should not have any transaction", kernelTransactions.activeTransactions(), is(empty()));
    }

    @Test
    public void exceptionWhenStartingNewTransactionOnShutdownInstance() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        SecurityContext securityContext = mock( SecurityContext.class );

        availabilityGuard.shutdown();

        expectedException.expect( DatabaseShutdownException.class );
        kernelTransactions.newInstance( KernelTransaction.Type.explicit, securityContext, 0L );
    }

    @Test
    public void exceptionWhenStartingNewTransactionOnStoppedKernelTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        SecurityContext securityContext = mock( SecurityContext.class );

        t2.execute( (OtherThreadExecutor.WorkerCommand<Void,Void>) state ->
        {
            stopKernelTransactions( kernelTransactions );
            return null;
        } ).get();

        expectedException.expect( IllegalStateException.class );
        kernelTransactions.newInstance( KernelTransaction.Type.explicit, securityContext, 0L );
    }

    @Test
    public void startNewTransactionOnRestartedKErnelTransactions() throws Throwable
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        SecurityContext securityContext = mock( SecurityContext.class );

        kernelTransactions.stop();
        kernelTransactions.start();
        assertNotNull( "New transaction created by restarted kernel transactions component.",
                kernelTransactions.newInstance( KernelTransaction.Type.explicit, securityContext, 0L ) );
    }

    private void stopKernelTransactions( KernelTransactions kernelTransactions )
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
            kernelTransactions.newInstance( KernelTransaction.Type.explicit, AUTH_DISABLED, 0L ).close();
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

    private static KernelTransactions newTestKernelTransactions() throws Throwable
    {
        return newKernelTransactions( true, mock( TransactionCommitProcess.class ), mock( SchemaResources.class ) );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess ) throws Throwable
    {
        return newKernelTransactions( false, commitProcess, mock( SchemaResources.class ) );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess,
            SchemaResources firstStoreStatements, SchemaResources... otherSchemaResources ) throws Throwable
    {
        return newKernelTransactions( false, commitProcess, firstStoreStatements, otherSchemaResources );
    }

    private static KernelTransactions newKernelTransactions( boolean testKernelTransactions,
            TransactionCommitProcess commitProcess, SchemaResources firstStoreStatements,
            SchemaResources... otherSchemaResources ) throws Throwable
    {
        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        StoreReadLayer readLayer = mock( StoreReadLayer.class );
        when( readLayer.schemaResources() ).thenReturn( firstStoreStatements, otherSchemaResources );

        StorageEngine storageEngine = mock( StorageEngine.class );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
        doAnswer( invocation ->
        {
            invocation.getArgumentAt( 0, Collection.class ).add( mock( StorageCommand.class ) );
            return null;
        } ).when( storageEngine ).createCommands(
                anyCollection(),
                any( ReadableTransactionState.class ),
                any( SchemaResources.class ),
                any( ResourceLocker.class ),
                anyLong() );

        return newKernelTransactions( locks, storageEngine, commitProcess, testKernelTransactions );
    }

    private static KernelTransactions newKernelTransactions( Locks locks, StorageEngine storageEngine,
            TransactionCommitProcess commitProcess, boolean testKernelTransactions ) throws Throwable
    {
        LifeSupport life = new LifeSupport();
        life.start();

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 0, 0, 0 ) );

        Tracers tracers = new Tracers( "null", NullLog.getInstance(), new Monitors(), mock( JobScheduler.class ) );
        StatementLocksFactory statementLocksFactory = new SimpleStatementLocksFactory( locks );

        StatementOperationContainer statementOperationsContianer = new StatementOperationContainer( null, null );
        KernelTransactions transactions;
        if ( testKernelTransactions )
        {
            transactions = createTestTransactions( storageEngine, commitProcess, transactionIdStore, tracers,
                    statementLocksFactory, statementOperationsContianer, clock, availabilityGuard );
        }
        else
        {
            transactions = createTransactions( storageEngine, commitProcess, transactionIdStore, tracers,
                    statementLocksFactory, statementOperationsContianer, clock, availabilityGuard );
        }
        transactions.start();
        return transactions;
    }

    private static KernelTransactions createTransactions( StorageEngine storageEngine,
            TransactionCommitProcess commitProcess, TransactionIdStore transactionIdStore, Tracers tracers,
            StatementLocksFactory statementLocksFactory, StatementOperationContainer statementOperationsContianer,
            Clock clock, AvailabilityGuard availabilityGuard )
    {
        return new KernelTransactions( statementLocksFactory,
                null, statementOperationsContianer, null, DEFAULT,
                commitProcess, null, null, new TransactionHooks(), mock( TransactionMonitor.class ),
                availabilityGuard,
                tracers, storageEngine, new Procedures(), transactionIdStore, clock, new CanWrite() );
    }

    private static TestKernelTransactions createTestTransactions( StorageEngine storageEngine,
            TransactionCommitProcess commitProcess, TransactionIdStore transactionIdStore, Tracers tracers,
            StatementLocksFactory statementLocksFactory, StatementOperationContainer statementOperationsContianer,
            Clock clock, AvailabilityGuard availabilityGuard )
    {
        return new TestKernelTransactions( statementLocksFactory, null, statementOperationsContianer,
                null, DEFAULT,
                commitProcess, null, null, new TransactionHooks(), mock( TransactionMonitor.class ),
                availabilityGuard, tracers, storageEngine, new Procedures(), transactionIdStore, clock,
                new CanWrite() );
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
                    slot[0] = ((TransactionToApply) invocation.getArguments()[0]).transactionRepresentation();
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

    private KernelTransaction getKernelTransaction( KernelTransactions transactions )
    {
        return transactions.newInstance( KernelTransaction.Type.implicit, AnonymousContext.none(), 0L );
    }

    private static class TestKernelTransactions extends KernelTransactions
    {
        TestKernelTransactions( StatementLocksFactory statementLocksFactory,
                ConstraintIndexCreator constraintIndexCreator, StatementOperationContainer statementOperationsContianer,
                SchemaWriteGuard schemaWriteGuard, TransactionHeaderInformationFactory txHeaderFactory,
                TransactionCommitProcess transactionCommitProcess, IndexConfigStore indexConfigStore,
                LegacyIndexProviderLookup legacyIndexProviderLookup, TransactionHooks hooks,
                TransactionMonitor transactionMonitor, AvailabilityGuard availabilityGuard, Tracers tracers,
                StorageEngine storageEngine, Procedures procedures, TransactionIdStore transactionIdStore, Clock clock,
                AccessCapability accessCapability )
        {
            super( statementLocksFactory, constraintIndexCreator, statementOperationsContianer, schemaWriteGuard,
                    txHeaderFactory, transactionCommitProcess, indexConfigStore, legacyIndexProviderLookup, hooks,
                    transactionMonitor, availabilityGuard, tracers, storageEngine, procedures, transactionIdStore,
                    clock,
                    accessCapability );
        }

        @Override
        KernelTransactionHandle createHandle( KernelTransactionImplementation tx )
        {
            return new TestKernelTransactionHandle( tx );
        }
    }
}
