/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class StopCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public StopCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void releaseWriteLockWaitersOnStop()
    {
        // given
        clientA.acquireShared( NODE, 1L );
        clientB.acquireShared( NODE, 2L );
        clientC.acquireShared( NODE, 3L );
        acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();
        acquireExclusive( clientC, NODE, 1L ).callAndAssertWaiting();

        // when
        clientC.stop();
        clientB.stop();
        clientA.stop();

        // all locks clients should be stopped at this point and all clients should still hold their shared locks
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        Assert.assertEquals( 3, lockCountVisitor.getLockCount() );
    }

    @Test
    public void releaseReadLockWaitersOnStop()
    {  // given
        clientA.acquireExclusive( NODE, 1L );
        clientB.acquireExclusive( NODE, 2L );
        acquireShared( clientB, NODE, 1L ).callAndAssertWaiting();

        // when
        clientB.stop();
        clientA.stop();

        // all locks clients should be stopped at this point and all clients should still hold their exclusive locks
        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        Assert.assertEquals( 2, lockCountVisitor.getLockCount() );
    }
}
