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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ReentrantLockService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.lock.ResourceLocker;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

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
        assertThat( registry.activeTransactions(), equalTo( asSet( second, third ) ) );
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

        assertTrue( leftOpen.shouldBeTerminated() );
    }

    @Test
    public void shouldIncludeRandomBytesInAdditionalHeader() throws Exception
    {
        // Given
        TransactionRepresentation[] transactionRepresentation = new TransactionRepresentation[1];

        KernelTransactions registry = newKernelTransactions( newRememberingCommitProcess( transactionRepresentation ) );

        // When
        try ( KernelTransaction transaction = registry.newInstance() )
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
    public void transactionCloseRemovesTxFromActiveTransactions() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = kernelTransactions.newInstance();
        KernelTransaction tx2 = kernelTransactions.newInstance();
        KernelTransaction tx3 = kernelTransactions.newInstance();

        kernelTransactions.transactionClosed( tx1 );
        kernelTransactions.transactionClosed( tx3 );

        assertEquals( asSet( tx2 ), kernelTransactions.activeTransactions() );
    }

    @Test
    public void transactionRemovesItselfFromActiveTransactions() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = kernelTransactions.newInstance();
        KernelTransaction tx2 = kernelTransactions.newInstance();
        KernelTransaction tx3 = kernelTransactions.newInstance();

        tx2.close();

        assertEquals( asSet( tx1, tx3 ), kernelTransactions.activeTransactions() );
    }

    @Test
    public void exceptionIsThrownWhenUnknownTxIsClosed() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        try
        {
            kernelTransactions.transactionClosed( mock( KernelTransactionImplementation.class ) );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void disposeAllMarksAllTransactionsForTermination() throws Exception
    {
        KernelTransactions kernelTransactions = newKernelTransactions();

        KernelTransaction tx1 = kernelTransactions.newInstance();
        KernelTransaction tx2 = kernelTransactions.newInstance();
        KernelTransaction tx3 = kernelTransactions.newInstance();

        kernelTransactions.disposeAll();

        assertTrue( tx1.shouldBeTerminated() );
        assertTrue( tx2.shouldBeTerminated() );
        assertTrue( tx3.shouldBeTerminated() );
    }

    private static KernelTransactions newKernelTransactions() throws Exception
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ) );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess ) throws Exception
    {
        LifeSupport life = new LifeSupport();
        life.start();

        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        NeoStores neoStores = mock( NeoStores.class );

        StoreStatement storeStatement = new StoreStatement( neoStores, new ReentrantLockService(),
                mock( IndexReaderFactory.class ), null );
        StoreReadLayer readLayer = mock( StoreReadLayer.class );
        when( readLayer.acquireStatement() ).thenReturn( storeStatement );

        StorageEngine storageEngine = mock( StorageEngine.class );
        when( storageEngine.storeReadLayer() ).thenReturn( readLayer );
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                invocation.getArgumentAt( 0, Collection.class ).add( mock( StorageCommand.class ) );
                return null;
            }
        } ).when( storageEngine ).createCommands(
                anyCollection(),
                any( ReadableTransactionState.class ),
                any( ResourceLocker.class ),
                anyLong() );

        Tracers tracers =
                new Tracers( "null", NullLog.getInstance(), mock( Monitors.class ), mock( JobScheduler.class ) );
        return new KernelTransactions( locks,
                null, null, null, TransactionHeaderInformationFactory.DEFAULT,
                commitProcess, null,
                null, new TransactionHooks(), mock( TransactionMonitor.class ), life,
                tracers, storageEngine, new Procedures(), metaDataStore );
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
}
