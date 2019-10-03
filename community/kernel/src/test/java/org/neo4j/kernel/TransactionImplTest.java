/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.token.TokenHolders;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TransactionImplTest
{
    private final TokenHolders tokenHolders = mock( TokenHolders.class );
    private final QueryExecutionEngine engine = mock( QueryExecutionEngine.class );
    private final TransactionalContextFactory contextFactory = mock( TransactionalContextFactory.class );
    private final DatabaseAvailabilityGuard availabilityGuard = mock( DatabaseAvailabilityGuard.class );

    @Test
    void shouldThrowTransientExceptionOnTransientKernelException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransactionFailureException( Status.Transaction.ConstraintsChanged,
                "Proving that transaction does the right thing" ) ).when( kernelTransaction ).close();
        TransactionImpl transaction = new TransactionImpl( tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction );

        // WHEN
        transaction.commit();
    }

    @Test
    void shouldThrowTransactionExceptionOnTransientKernelException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new RuntimeException( "Just a random failure" ) ).when( kernelTransaction ).close();
        TransactionImpl transaction = new TransactionImpl( tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction );

        // WHEN
        transaction.commit();
    }

    @Test
    void shouldLetThroughTransientFailureException() throws Exception
    {
        // GIVEN
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.isOpen() ).thenReturn( true );
        doThrow( new TransientDatabaseFailureException( "Just a random failure" ) ).when( kernelTransaction ).close();
        TransactionImpl transaction = new TransactionImpl( tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction );

        // WHEN
        transaction.commit();
    }

    @Test
    void shouldShowTransactionTerminatedExceptionAsTransient() throws Exception
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        doReturn( true ).when( kernelTransaction ).isOpen();
        RuntimeException error = new TransactionTerminatedException( Status.Transaction.Terminated );
        doThrow( error ).when( kernelTransaction ).close();
        TransactionImpl transaction = new TransactionImpl( tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction );

        transaction.commit();
    }

    @Test
    void shouldReturnTerminationReason()
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.getReasonIfTerminated() ).thenReturn( Optional.empty() )
                .thenReturn( Optional.of( Status.Transaction.Terminated ) );

        TransactionImpl tx = new TransactionImpl( tokenHolders, contextFactory, availabilityGuard, engine, kernelTransaction );

        Optional<Status> terminationReason1 = tx.terminationReason();
        Optional<Status> terminationReason2 = tx.terminationReason();

        assertFalse( terminationReason1.isPresent() );
        assertTrue( terminationReason2.isPresent() );
        assertEquals( Status.Transaction.Terminated, terminationReason2.get() );
    }
}
