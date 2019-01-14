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
package org.neo4j.kernel.guard;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Answers;

import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactionTestBase;
import org.neo4j.kernel.impl.locking.StatementLocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class TerminationGuardTest extends KernelTransactionTestBase
{

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void allowToProceedWhenTransactionIsNotTerminated()
    {
        TerminationGuard terminationGuard = buildGuard();

        KernelTransactionImplementation kernelTransaction = getKernelTransaction();
        terminationGuard.check( kernelTransaction );
    }

    @Test
    public void throwExceptionWhenCheckTerminatedTransaction()
    {
        TerminationGuard terminationGuard = buildGuard();

        KernelTransactionImplementation kernelTransaction = getKernelTransaction();
        kernelTransaction.markForTermination( Status.Transaction.Terminated );

        expectedException.expect( TransactionTerminatedException.class );

        terminationGuard.check( kernelTransaction );
    }

    @Test
    public void throwExceptionWhenCheckTerminatedStatement()
    {
        TerminationGuard terminationGuard = buildGuard();

        KernelTransactionImplementation kernelTransaction = getKernelTransaction();
        try ( KernelStatement kernelStatement = kernelTransaction.acquireStatement() )
        {
            kernelTransaction.markForTermination( Status.Transaction.Terminated );
            expectedException.expect( TransactionTerminatedException.class );

            terminationGuard.check( kernelTransaction );
        }
    }

    private KernelTransactionImplementation getKernelTransaction()
    {
        KernelTransactionImplementation transaction = newNotInitializedTransaction();
        StatementLocks statementLocks = mock( StatementLocks.class, Answers.RETURNS_DEEP_STUBS );
        when( statementLocks.pessimistic().getLockSessionId() ).thenReturn( 1 );
        transaction.initialize( 1L, 2L, statementLocks, KernelTransaction.Type.implicit,
                AUTH_DISABLED, 1L, 1L );
        return transaction;
    }

    private TerminationGuard buildGuard()
    {
        return new TerminationGuard();
    }
}
