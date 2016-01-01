/**
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
package org.neo4j.kernel.impl.api.integrationtest;

import javax.transaction.xa.XAException;

import org.junit.Test;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.TxState;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TransactionHookIT extends KernelIntegrationTest
{
    @Test
    public void shouldRecieveTxStateOnCommit() throws Exception
    {
        // Given
        TransactionHook hook = mock( TransactionHook.class );
        kernel.registerTransactionHook( hook );

        // When
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.nodeCreate();
        commit();

        // Then
        verify(hook).beforeCommit( any(TxState.class), any( KernelTransaction.class) );
        verify(hook).afterCommit( any( TxState.class ), any( KernelTransaction.class ), any(TransactionHook.Outcome.class) );
        verifyNoMoreInteractions( hook );
    }

    @Test
    public void shouldRollbackOnFailureInBeforeCommit() throws Exception
    {
        // Given
        TransactionHook hook = mock( TransactionHook.class );
        when( hook.beforeCommit( any( TxState.class ), any( KernelTransaction.class ) )).thenReturn( new TransactionHook.Outcome()

        {
            @Override
            public boolean isSuccessful()
            {
                return false;
            }

            @Override
            public Throwable failure()
            {
                return new Throwable();
            }
        } );
        kernel.registerTransactionHook( hook );

        // When
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.nodeCreate();

        try
        {
            commit();
            fail("Expected this to fail.");
        }
        catch(TransactionFailureException e)
        {
            XAException xaException = (XAException)e.getCause().getCause();
            assertThat( xaException.errorCode, equalTo(XAException.XA_RBOTHER) );
            assertThat( xaException.getCause().getMessage(), equalTo("Transaction handler failed.") );
        }

        // Then
        verify(hook).beforeCommit( any(TxState.class), any( KernelTransaction.class) );
        verify(hook).afterRollback( any( TxState.class ), any( KernelTransaction.class ), any(TransactionHook.Outcome.class) );
        verifyNoMoreInteractions( hook );
    }
}
