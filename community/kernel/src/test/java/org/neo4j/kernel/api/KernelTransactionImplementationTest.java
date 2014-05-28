/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.xa.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMonitor;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class KernelTransactionImplementationTest
{
    @Test
    public void shouldCommitSuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
            transaction.success();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( true );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackUnsuccessfulTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackFailedTransaction() throws Exception
    {
        // GIVEN
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
            transaction.failure();
        }

        // THEN
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    @Test
    public void shouldRollbackAndThrowOnFailedAndSuccess() throws Exception
    {
        // GIVEN
        boolean exceptionReceived = false;
        try ( KernelTransaction transaction = newTransaction() )
        {
            // WHEN
            transaction.failure();
            transaction.success();
        }
        catch ( TransactionFailureException e )
        {
            // Expected.
            exceptionReceived = true;
        }

        // THEN
        assertTrue( exceptionReceived );
        verify( transactionMonitor, times( 1 ) ).transactionFinished( false );
        verifyNoMoreInteractions( transactionMonitor );
    }

    private final NeoStore neoStore = mock( NeoStore.class );
    private final TransactionState txState = mock( TransactionState.class );
    private final TransactionHooks hooks = new TransactionHooks();
    private final TransactionRecordState recordState = mock( TransactionRecordState.class );
    private final TransactionMonitor transactionMonitor = mock( TransactionMonitor.class );

    @Before
    public void before()
    {
        when( txState.locks() ).thenReturn( new NoOpClient() );
        when( recordState.isReadOnly() ).thenReturn( true );
    }

    private KernelTransactionImplementation newTransaction()
    {
        return new KernelTransactionImplementation( null, false, null, null, null,
                null, null, recordState, null, neoStore, txState, hooks, null, null, null,
                transactionMonitor, neoStore );
    }
}
