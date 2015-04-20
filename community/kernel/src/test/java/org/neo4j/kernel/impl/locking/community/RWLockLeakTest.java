/*
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
package org.neo4j.kernel.impl.locking.community;

import static org.mockito.Mockito.mock;

import javax.transaction.Transaction;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RWLockLeakTest
{
    @Test
    public void assertWriteLockDoesNotLeakMemory() throws InterruptedException
    {
        final RagManager ragManager = new RagManager();
        final Object resource = new Object();
        final RWLock lock = new RWLock( resource, ragManager );
        final Transaction tx1 = mock( Transaction.class );
        
        lock.mark();
        lock.acquireWriteLock( tx1 );
        lock.mark();
        
        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseWriteLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }

    @Test
    public void assertReadLockDoesNotLeakMemory() throws InterruptedException
    {
        final RagManager ragManager = new RagManager();
        final Object resource = new Object();
        final RWLock lock = new RWLock( resource, ragManager );
        final Transaction tx1 = mock( Transaction.class );
        
        lock.mark();
        lock.acquireReadLock( tx1 );
        lock.mark();
        
        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseReadLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }
}
