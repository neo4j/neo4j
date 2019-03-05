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

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.lock.LockTracer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StatementLifecycleTest
{
    @Test
    void shouldReleaseStoreStatementOnlyWhenReferenceCountDownToZero()
    {
        // given
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        KernelStatement statement = getKernelStatement( transaction );
        statement.acquire();
        statement.acquire();

        // when
        statement.close();
        verify( transaction, never() ).releaseStatementResources();

        // then
        statement.close();
        verify( transaction ).releaseStatementResources();
    }

    @Test
    void shouldReleaseStoreStatementWhenForceClosingStatements()
    {
        // given
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.isSuccess() ).thenReturn( true );
        KernelStatement statement = getKernelStatement( transaction );
        statement.acquire();

        // when
        assertThrows( KernelStatement.StatementNotClosedException.class, statement::forceClose );

        // then
        verify( transaction ).releaseStatementResources();
    }

    private KernelStatement getKernelStatement( KernelTransactionImplementation transaction )
    {
        return new KernelStatement( transaction, null,
                LockTracer.NONE, mock( StatementOperationParts.class ), new ClockContext(), EmptyVersionContextSupplier.EMPTY );
    }
}
