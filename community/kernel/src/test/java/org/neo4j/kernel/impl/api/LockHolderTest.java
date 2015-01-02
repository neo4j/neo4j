/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import javax.transaction.Transaction;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import org.neo4j.kernel.api.exceptions.ReleaseLocksFailedKernelException;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.SchemaLock;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockNotFoundException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class LockHolderTest
{
    @Test
    public void shouldAcquireSchemaReadLock()
    {
        // GIVEN
        LockManager mgr = mock( LockManager.class );
        Transaction tx = mock( Transaction.class );
        NodeManager nm = mock( NodeManager.class );
        LockHolder holder = new LockHolderImpl( mgr, tx, nm );

        // WHEN
        holder.acquireSchemaReadLock();


        // THEN
        Mockito.verify( mgr ).getReadLock( any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldAcquireSchemaWriteLock()
    {
        // GIVEN
        LockManager mgr = mock( LockManager.class );
        Transaction tx = mock( Transaction.class );
        NodeManager nm = mock( NodeManager.class );
        LockHolder holder = new LockHolderImpl( mgr, tx, nm );

        // WHEN
        holder.acquireSchemaWriteLock();


        // THEN
        Mockito.verify( mgr ).getWriteLock( any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaReadLockOnRelease() throws ReleaseLocksFailedKernelException
    {
        // GIVEN
        LockManager mgr = mock( LockManager.class );
        Transaction tx = mock( Transaction.class );
        NodeManager nm = mock( NodeManager.class );
        LockHolder holder = new LockHolderImpl( mgr, tx, nm );

        // WHEN
        holder.acquireSchemaReadLock();
        holder.releaseLocks();


        // THEN
        Mockito.verify( mgr ).releaseReadLock( any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaWriteLockOnRelease() throws ReleaseLocksFailedKernelException
    {
        // GIVEN
        LockManager mgr = mock( LockManager.class );
        Transaction tx = mock( Transaction.class );
        NodeManager nm = mock( NodeManager.class );
        LockHolder holder = new LockHolderImpl( mgr, tx, nm );

        // WHEN
        holder.acquireSchemaWriteLock();
        holder.releaseLocks();


        // THEN
        Mockito.verify( mgr ).releaseWriteLock( any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldThrowExceptionWhenAttemptingToReleaseUnknownLocks()
    {
        // For instance, this can happen if the locks were taken prior to a master-switch,
        // and then attempted released after the master-switch.

        // GIVEN
        LockManager mgr = mock( LockManager.class );
        Transaction tx = mock( Transaction.class );
        NodeManager nm = mock( NodeManager.class );
        LockHolder holder = new LockHolderImpl( mgr, tx, nm );

        doThrow( new LockNotFoundException( "Sad face" ) )
                .when( mgr )
                .releaseWriteLock( anyObject(), any( Transaction.class ) );
        doThrow( new LockNotFoundException( "Sad face" ) )
                .when( mgr )
                .releaseReadLock( anyObject(), any( Transaction.class ) );

        // WHEN
        holder.acquireSchemaReadLock();
        holder.acquireSchemaReadLock();
        holder.acquireNodeWriteLock( 1337 );

        // THEN
        try
        {
            holder.releaseLocks();
            fail( "Expected releaseLocks to throw" );
        }
        catch ( ReleaseLocksFailedKernelException e )
        {
            assertThat( e.getMessage(), containsString( "[READ SchemaLocks: 2, WRITE NodeLocks: 1]" ) );
        }
    }
}
