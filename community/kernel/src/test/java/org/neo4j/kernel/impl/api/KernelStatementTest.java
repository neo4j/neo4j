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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageStatement;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KernelStatementTest
{
    @Test
    public void shouldReleaseStorageStatementWhenForceClosed()
    {
        // given
        StorageStatement storeStatement = mock( StorageStatement.class );
        KernelStatement statement = new KernelStatement( mock( KernelTransactionImplementation.class ),
                null, storeStatement, LockTracer.NONE,
                mock( StatementOperationParts.class ), new ClockContext(), EmptyVersionContextSupplier.EMPTY );
        statement.acquire();

        // when
        try
        {
            statement.forceClose();
        }
        catch ( KernelStatement.StatementNotClosedException ignored )
        {
            // ignore
        }

        // then
        verify( storeStatement ).release();
    }

    @Test( expected = NotInTransactionException.class )
    public void assertStatementIsNotOpenWhileAcquireIsNotInvoked()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        TxStateHolder txStateHolder = mock( TxStateHolder.class );
        StorageStatement storeStatement = mock( StorageStatement.class );
        KernelStatement statement = new KernelStatement( transaction, txStateHolder,
                storeStatement, LockTracer.NONE, mock( StatementOperationParts.class ),
                new ClockContext(), EmptyVersionContextSupplier.EMPTY );

        statement.assertOpen();
    }

    @Test
    public void reportQueryWaitingTimeToTransactionStatisticWhenFinishQueryExecution()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        TxStateHolder txStateHolder = mock( TxStateHolder.class );
        StorageStatement storeStatement = mock( StorageStatement.class );

        KernelTransactionImplementation.Statistics statistics = new KernelTransactionImplementation.Statistics( transaction,
                new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ) );
        when( transaction.getStatistics() ).thenReturn( statistics );
        when( transaction.executingQueries() ).thenReturn( ExecutingQueryList.EMPTY );

        KernelStatement statement = new KernelStatement( transaction, txStateHolder,
                storeStatement, LockTracer.NONE, mock( StatementOperationParts.class ),
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
