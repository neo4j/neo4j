/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextFactory;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.test.Race;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;

public class KernelTransactionsTest
{
    @Test
    public void shouldListActiveTransactions() throws Exception
    {
        // Given
        KernelTransactions registry = newKernelTransactions();

        // When
        KernelTransaction first = registry.newInstance();
        KernelTransaction second = registry.newInstance();
        KernelTransaction third = registry.newInstance();

        first.close();

        // Then
        assertThat( asUniqueSet( registry.activeTransactions() ), equalTo( asSet( second, third ) ) );
    }

    @Test
    public void shouldDisposeTransactionsWhenAsked() throws Exception
    {
        // Given
        KernelTransactions registry = newKernelTransactions();

        registry.disposeAll();

        KernelTransaction first = registry.newInstance();
        KernelTransaction second = registry.newInstance();
        KernelTransaction leftOpen = registry.newInstance();
        first.close();
        second.close();

        // When
        registry.disposeAll();

        // Then
        KernelTransaction postDispose = registry.newInstance();
        assertThat( postDispose, not( equalTo( first ) ) );
        assertThat( postDispose, not( equalTo( second ) ) );

        assertTrue( leftOpen.getReasonIfTerminated() != null );
    }

    @Test
    public void shouldIncludeRandomBytesInAdditionalHeader() throws TransactionFailureException
    {
        // Given
        TransactionRepresentation[] transactionRepresentation = new TransactionRepresentation[1];

        KernelTransactions registry = newKernelTransactions(
                newRememberingCommitProcess( transactionRepresentation ), newMockContextFactoryWithChanges() );

        // When
        KernelTransaction transaction = registry.newInstance();
        transaction.success();
        transaction.close();

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
        KernelTransaction a = transactions.newInstance();

        // WHEN
        a.close();
        KernelTransaction b = transactions.newInstance();

        // THEN
        assertSame( a, b );
    }

    @Test
    public void shouldTellWhenTransactionsFromSnapshotHaveBeenClosed() throws Exception
    {
        // GIVEN
        KernelTransactions transactions = newKernelTransactions();
        KernelTransaction a = transactions.newInstance();
        KernelTransaction b = transactions.newInstance();
        KernelTransaction c = transactions.newInstance();
        KernelTransactionsSnapshot snapshot = transactions.get();
        assertFalse( snapshot.allClosed() );

        // WHEN a gets closed
        a.close();
        assertFalse( snapshot.allClosed() );

        // WHEN c gets closed and (test knowing too much) that instance getting reused in another transaction "d".
        c.close();
        KernelTransaction d = transactions.newInstance();
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
            race.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while ( !end.get() )
                    {
                        try ( KernelTransaction transaction = transactions.newInstance() )
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
                }
            } );
        }

        // Just checks snapshots
        race.addContestant( new Runnable()
        {
            @Override
            public void run()
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
            }
        } );

        // WHEN
        race.go();
    }

    @Test
    public void threadThatBlocksNewTxsCantStartNewTxs() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();
        kernelTransactions.blockNewTransactions();
        try
        {
            kernelTransactions.newInstance();
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

    private static KernelTransactions newKernelTransactions()
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ), newMockContextFactory() );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess,
            NeoStoreTransactionContextFactory contextSupplier )
    {
        LifeSupport life = new LifeSupport();
        life.start();

        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        StoreReadLayer readLayer = mock( StoreReadLayer.class );
        when( readLayer.acquireStatement() ).thenReturn( mock( StoreStatement.class ) );

        NeoStores neoStores = mock( NeoStores.class );
        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        when( metaDataStore.getLastCommittedTransaction() ).thenReturn( new TransactionId( 2, 3, 4 ) );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
        return new KernelTransactions( contextSupplier, neoStores, new SimpleStatementLocksFactory( locks ),
                mock( IntegrityValidator.class ), null, null, null, null, null, null, null,
                TransactionHeaderInformationFactory.DEFAULT, readLayer, commitProcess, null,
                null, new TransactionHooks(), mock( ConstraintSemantics.class ), mock( TransactionMonitor.class ),
                life, new ProcedureCache(), new Config(), new Tracers( "null", NullLog.getInstance() ),
                Clock.SYSTEM_CLOCK );
    }

    private static TransactionCommitProcess newRememberingCommitProcess( final TransactionRepresentation[] slot )
            throws TransactionFailureException

    {
        TransactionCommitProcess commitProcess = mock( TransactionCommitProcess.class );

        when( commitProcess.commit(
                any( TransactionRepresentation.class ), any( LockGroup.class ), any( CommitEvent.class ),
                any( TransactionApplicationMode.class ) ) )
                .then( new Answer<Long>()
                {
                    @Override
                    public Long answer( InvocationOnMock invocation ) throws Throwable
                    {
                        slot[0] = ((TransactionRepresentation) invocation.getArguments()[0]);
                        return 1L;
                    }
                } );

        return commitProcess;
    }

    private static NeoStoreTransactionContextFactory newMockContextFactory()
    {
        NeoStoreTransactionContextFactory factory = mock( NeoStoreTransactionContextFactory.class );
        NeoStoreTransactionContext context = mock( NeoStoreTransactionContext.class, RETURNS_MOCKS );
        when( factory.newInstance() ).thenReturn( context );
        return factory;
    }

    @SuppressWarnings( "unchecked" )
    private static NeoStoreTransactionContextFactory newMockContextFactoryWithChanges()
    {
        NeoStoreTransactionContextFactory factory = mock( NeoStoreTransactionContextFactory.class );

        NeoStoreTransactionContext context = mock( NeoStoreTransactionContext.class, RETURNS_MOCKS );
        when( context.hasChanges() ).thenReturn( true );

        RecordAccess<Long,NodeRecord,Void> recordChanges = mock( RecordAccess.class );
        when( recordChanges.changeSize() ).thenReturn( 1 );

        RecordProxy<Long,NodeRecord,Void> recordChange = mock( RecordProxy.class );
        when( recordChange.forReadingLinkage() ).thenReturn( new NodeRecord( 1, false, 1, 1 ) );

        when( recordChanges.changes() ).thenReturn( Iterables.option( recordChange ) );
        when( context.getNodeRecords() ).thenReturn( recordChanges );

        when( factory.newInstance() ).thenReturn( context );
        return factory;
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
                return kernelTransactions.newInstance();
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
}
