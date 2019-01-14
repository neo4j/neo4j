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

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.PlaceboTransaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPlaceboTransaction
{
    private Transaction placeboTx;
    private Node resource;
    private KernelTransaction kernelTransaction;
    private Locks locks;

    @Before
    public void before()
    {
        ThreadToStatementContextBridge bridge = mock( ThreadToStatementContextBridge.class );
        Statement statement = mock( Statement.class );
        when( bridge.get() ).thenReturn( statement );
        kernelTransaction = spy( KernelTransaction.class );
        locks = mock( Locks.class );
        when(kernelTransaction.locks()).thenReturn( locks );
        placeboTx = new PlaceboTransaction( kernelTransaction );
        resource = mock( Node.class );
        when( resource.getId() ).thenReturn( 1L );
    }

    @Test
    public void shouldRollbackParentByDefault()
    {
        // When
        placeboTx.close();

        // Then
        verify( kernelTransaction ).failure();
    }

    @Test
    public void shouldRollbackParentIfFailureCalled()
    {
        // When
        placeboTx.failure();
        placeboTx.close();

        // Then
        verify( kernelTransaction, times(2) ).failure(); // We accept two calls to failure, since KernelTX#failure is idempotent
    }

    @Test
    public void shouldNotRollbackParentIfSuccessCalled()
    {
        // When
        placeboTx.success();
        placeboTx.close();

        // Then
        verify( kernelTransaction, never() ).failure();
    }

    @Test
    public void successCannotOverrideFailure()
    {
        // When
        placeboTx.failure();
        placeboTx.success();
        placeboTx.close();

        // Then
        verify( kernelTransaction ).failure();
        verify( kernelTransaction, never() ).success();
    }

    @Test
    public void canAcquireReadLock()
    {
        // when
        placeboTx.acquireReadLock( resource );

        // then
        verify( locks ).acquireSharedNodeLock( resource.getId() );
    }

    @Test
    public void canAcquireWriteLock()
    {
        // when
        placeboTx.acquireWriteLock( resource );

        // then
        verify( locks ).acquireExclusiveNodeLock( resource.getId() );
    }

    @Test
    public void shouldReturnTerminationReason()
    {
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        when( kernelTransaction.getReasonIfTerminated() ).thenReturn( Optional.empty() )
                .thenReturn( Optional.of( Status.Transaction.Interrupted ) );

        PlaceboTransaction tx = new PlaceboTransaction( kernelTransaction );

        Optional<Status> terminationReason1 = tx.terminationReason();
        Optional<Status> terminationReason2 = tx.terminationReason();

        assertFalse( terminationReason1.isPresent() );
        assertTrue( terminationReason2.isPresent() );
        assertEquals( Status.Transaction.Interrupted, terminationReason2.get() );
    }
}
