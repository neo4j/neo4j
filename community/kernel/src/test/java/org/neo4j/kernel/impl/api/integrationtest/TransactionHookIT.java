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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.TransactionHook;
import org.neo4j.kernel.api.txstate.ReadableTxState;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

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
        verify( hook ).beforeCommit( any( ReadableTxState.class ), any( KernelTransaction.class ),
                any( StoreReadLayer.class ) );
        verify( hook ).afterCommit( any( ReadableTxState.class ), any( KernelTransaction.class ),
                any( TransactionHook.Outcome.class ) );
        verifyNoMoreInteractions( hook );
    }

    @Test
    public void shouldRollbackOnFailureInBeforeCommit() throws Exception
    {
        // Given
        TransactionHook hook = mock( TransactionHook.class );
        final String message = "Original";
        when( hook.beforeCommit( any( ReadableTxState.class ), any( KernelTransaction.class ),
                any( StoreReadLayer.class ) ) ).thenReturn( new TransactionHook.Outcome()
        {
            @Override
            public boolean isSuccessful()
            {
                return false;
            }

            @Override
            public Throwable failure()
            {
                return new Throwable( message );
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
        catch ( org.neo4j.kernel.api.exceptions.TransactionFailureException e )
        {
            assertThat( e.getCause().getMessage(), equalTo( "Transaction handler failed." ) );
            assertThat( e.getCause().getCause().getMessage(), equalTo( message ) );
        }
        // Then
        verify( hook ).beforeCommit( any( ReadableTxState.class ), any( KernelTransaction.class ),
                any( StoreReadLayer.class ) );
        verify( hook ).afterRollback( any( ReadableTxState.class ), any( KernelTransaction.class ),
                any( TransactionHook.Outcome.class ) );
        verifyNoMoreInteractions( hook );
    }
}
