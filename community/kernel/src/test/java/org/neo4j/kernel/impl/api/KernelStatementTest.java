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

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageStatement;

import static java.util.Optional.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.kernel.impl.api.ExecutingQueryList.EMPTY;
import static org.neo4j.kernel.impl.api.KernelStatement.StatementNotClosedException;
import static org.neo4j.kernel.impl.api.KernelTransactionImplementation.Statistics;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.resources.CpuClock.NOT_AVAILABLE;

public class KernelStatementTest
{
    @Test
    public void shouldThrowTerminateExceptionWhenTransactionTerminated()
    {
        assertThrows( TransactionTerminatedException.class, () -> {
            KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
            when( transaction.getReasonIfTerminated() ).thenReturn( of( Terminated ) );
            when( transaction.securityContext() ).thenReturn( AUTH_DISABLED );

            KernelStatement statement =
                    new KernelStatement( transaction, null, mock( StorageStatement.class ), null, new CanWrite(), NONE,
                            mock( StatementOperationParts.class ), new ClockContext(), EmptyVersionContextSupplier.EMPTY );
            statement.acquire();

            statement.readOperations().nodeExists( 0 );
        } );
    }

    @Test
    public void shouldReleaseStorageStatementWhenForceClosed()
    {
        // given
        StorageStatement storeStatement = mock( StorageStatement.class );
        KernelStatement statement = new KernelStatement( mock( KernelTransactionImplementation.class ),
                null, storeStatement, new Procedures(), new CanWrite(), LockTracer.NONE,
                mock( StatementOperationParts.class ), new ClockContext(), EmptyVersionContextSupplier.EMPTY );
        statement.acquire();

        // when
        try
        {
            statement.forceClose();
        }
        catch ( StatementNotClosedException ignored )
        {
            // ignore
        }

        // then
        verify( storeStatement ).release();
    }

    @Test
    public void assertStatementIsNotOpenWhileAcquireIsNotInvoked()
    {
        assertThrows( NotInTransactionException.class, () -> {
            KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
            TxStateHolder txStateHolder = mock( TxStateHolder.class );
            StorageStatement storeStatement = mock( StorageStatement.class );
            AccessCapability accessCapability = mock( AccessCapability.class );
            Procedures procedures = mock( Procedures.class );
            KernelStatement statement = new KernelStatement( transaction, txStateHolder,
                    storeStatement, procedures, accessCapability, LockTracer.NONE, mock( StatementOperationParts.class ),
                    new ClockContext(), EmptyVersionContextSupplier.EMPTY );

            statement.assertOpen();
        } );
    }

    @Test
    public void reportQueryWaitingTimeToTransactionStatisticWhenFinishQueryExecution()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        TxStateHolder txStateHolder = mock( TxStateHolder.class );
        StorageStatement storeStatement = mock( StorageStatement.class );
        AccessCapability accessCapability = mock( AccessCapability.class );
        Procedures procedures = mock( Procedures.class );

        Statistics statistics = new Statistics( transaction, new AtomicReference<>( NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ) );
        when( transaction.getStatistics() ).thenReturn( statistics );
        when( transaction.executingQueries() ).thenReturn( EMPTY );

        KernelStatement statement = new KernelStatement( transaction, txStateHolder,
                storeStatement, procedures, accessCapability, LockTracer.NONE, mock( StatementOperationParts.class ),
                new ClockContext(), EmptyVersionContextSupplier.EMPTY );
        statement.acquire();

        ExecutingQuery query = getQueryWithWaitingTime();
        ExecutingQuery query2 = getQueryWithWaitingTime();
        ExecutingQuery query3 = getQueryWithWaitingTime();

        statement.stopQueryExecution( query );
        statement.stopQueryExecution( query2 );
        statement.stopQueryExecution( query3 );

        assertEquals( 3, statistics.getWaitingTimeNanos( 1 ) );
    }

    private ExecutingQuery getQueryWithWaitingTime()
    {
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        when( executingQuery.reportedWaitingTimeNanos() ).thenReturn( 1L );
        return executingQuery;
    }
}
