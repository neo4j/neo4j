/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextSupplier;
import org.neo4j.kernel.impl.transaction.state.RecordAccess;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.tracing.Tracers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
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

        assertTrue( leftOpen.shouldBeTerminated() );
    }

    @Test
    public void shouldIncludeRandomBytesInAdditionalHeader() throws TransactionFailureException
    {
        // Given
        TransactionRepresentation[] transactionRepresentation = new TransactionRepresentation[1];

        KernelTransactions registry = newKernelTransactions(
                newRememberingCommitProcess( transactionRepresentation ), newMockContextSupplierWithChanges() );

        // When
        KernelTransaction transaction = registry.newInstance();
        transaction.success();
        transaction.close();

        // Then
        byte[] additionalHeader = transactionRepresentation[0].additionalHeader();
        assertNotNull( additionalHeader );
        assertTrue( additionalHeader.length > 0 );
    }

    private static KernelTransactions newKernelTransactions()
    {
        return newKernelTransactions( mock( TransactionCommitProcess.class ), new MockContextSupplier() );
    }

    private static KernelTransactions newKernelTransactions( TransactionCommitProcess commitProcess,
                                                             NeoStoreTransactionContextSupplier contextSupplier )
    {
        LifeSupport life = new LifeSupport();
        life.start();

        Locks locks = mock( Locks.class );
        when( locks.newClient() ).thenReturn( mock( Locks.Client.class ) );

        return new KernelTransactions( contextSupplier, mock( NeoStore.class ), locks,
                mock( IntegrityValidator.class ), null, null, null, null, null, null, null,
                TransactionHeaderInformationFactory.DEFAULT, null, null, commitProcess, null,
                null, new TransactionHooks(), mock( TransactionMonitor.class ), life,
                new Tracers( "null", StringLogger.DEV_NULL ) );
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

    private static NeoStoreTransactionContextSupplier newMockContextSupplierWithChanges()
    {
        return new MockContextSupplier()
        {
            @Override
            @SuppressWarnings("unchecked")
            protected NeoStoreTransactionContext create()
            {
                NeoStoreTransactionContext context = super.create();
                when( context.hasChanges() ).thenReturn( true );

                RecordAccess<Long, NodeRecord, Void> recordChanges = mock( RecordAccess.class );
                when( recordChanges.changeSize() ).thenReturn( 1 );

                RecordProxy<Long, NodeRecord, Void> recordChange = mock( RecordProxy.class );
                when( recordChange.forReadingLinkage() ).thenReturn( new NodeRecord( 1, false, 1, 1 ) );

                when( recordChanges.changes() ).thenReturn( Iterables.option( recordChange ) );
                when( context.getNodeRecords() ).thenReturn( recordChanges );

                return context;
            }
        };
    }

    private static class MockContextSupplier extends NeoStoreTransactionContextSupplier
    {
        public MockContextSupplier()
        {
            super( null );
        }

        @Override
        protected NeoStoreTransactionContext create()
        {
            return mock( NeoStoreTransactionContext.class, RETURNS_MOCKS );
        }
    }
}
