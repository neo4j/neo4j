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

import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.storageengine.api.StorageStatement;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class StatementLifecycleTest
{
    @Test
    public void shouldReleaseStoreStatementOnlyWhenReferenceCountDownToZero()
    {
        // given
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        StorageStatement storageStatement = mock( StorageStatement.class );
        KernelStatement statement = getKernelStatement( transaction, storageStatement );
        statement.acquire();
        verify( storageStatement ).acquire();
        statement.acquire();

        // when
        statement.close();
        verifyNoMoreInteractions( storageStatement );

        // then
        statement.close();
        verify( storageStatement ).release();
    }

    @Test
    public void shouldReleaseStoreStatementWhenForceClosingStatements()
    {
        // given
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        StorageStatement storageStatement = mock( StorageStatement.class );
        KernelStatement statement = getKernelStatement( transaction, storageStatement );
        statement.acquire();

        // when
        try
        {
            statement.forceClose();
        }
        catch ( KernelStatement.StatementNotClosedException ignored )
        {
            //ignored
        }

        // then
        verify( storageStatement ).release();
    }

    private KernelStatement getKernelStatement( KernelTransactionImplementation transaction,
            StorageStatement storageStatement )
    {
        return new KernelStatement( transaction, null, storageStatement,
                LockTracer.NONE, mock( StatementOperationParts.class ), new ClockContext(), EmptyVersionContextSupplier.EMPTY );
    }
}
