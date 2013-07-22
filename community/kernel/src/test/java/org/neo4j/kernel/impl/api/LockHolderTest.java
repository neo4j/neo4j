/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.SchemaLock;
import org.neo4j.kernel.impl.transaction.LockManager;

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
        Mockito.verify( mgr ).getReadLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
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
        Mockito.verify( mgr ).getWriteLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaReadLockOnRelease()
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
        Mockito.verify( mgr ).releaseReadLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }

    @Test
    public void shouldReleaseSchemaWriteLockOnRelease()
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
        Mockito.verify( mgr ).releaseWriteLock( Matchers.any( SchemaLock.class ), Matchers.eq( tx ) );
    }
}
