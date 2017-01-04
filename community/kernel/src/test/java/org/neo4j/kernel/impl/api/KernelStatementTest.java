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

import org.junit.Test;

import java.util.Optional;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.storageengine.api.StorageStatement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class KernelStatementTest
{
    @Test(expected = TransactionTerminatedException.class)
    public void shouldThrowTerminateExceptionWhenTransactionTerminated() throws Exception
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.of( Status.Transaction.Terminated ) );
        when( transaction.securityContext() ).thenReturn( AUTH_DISABLED );

        KernelStatement statement = new KernelStatement( transaction, null, mock( StorageStatement.class ), null, new CanWrite(),
                LockTracer.NONE );
        statement.acquire();

        statement.readOperations().nodeExists( 0 );
    }

    @Test
    public void shouldReleaseStorageStatementWhenForceClosed() throws Exception
    {
        // given
        StorageStatement storeStatement = mock( StorageStatement.class );
        KernelStatement statement = new KernelStatement( mock( KernelTransactionImplementation.class ),
                null, storeStatement, new Procedures(), new CanWrite(), LockTracer.NONE );
        statement.acquire();

        // when
        statement.forceClose();

        // then
        verify( storeStatement ).release();
    }

    @Test(expected = NotInTransactionException.class)
    public void assertStatementIsNotOpenWhileAcquireIsNotInvoked()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        TxStateHolder txStateHolder = mock( TxStateHolder.class );
        StorageStatement storeStatement = mock( StorageStatement.class );
        AccessCapability accessCapability = mock( AccessCapability.class );
        Procedures procedures = mock( Procedures.class );
        KernelStatement statement = new KernelStatement( transaction, txStateHolder,
                storeStatement, procedures, accessCapability, LockTracer.NONE );

        statement.assertOpen();
    }
}
