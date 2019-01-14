/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import org.neo4j.kernel.impl.locking.Locks;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.exceptions.Status.General.ForbiddenOnReadOnlyDatabase;
import static org.neo4j.kernel.api.exceptions.Status.statusCodeOf;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

public class ReadReplicaLockManagerTest
{
    private ReadReplicaLockManager lockManager = new ReadReplicaLockManager();
    private Locks.Client lockClient = lockManager.newClient();

    @Test
    public void shouldThrowOnAcquireExclusiveLock()
    {
        try
        {
            // when
            lockClient.acquireExclusive( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
        }
    }

    @Test
    public void shouldThrowOnTryAcquireExclusiveLock()
    {
        try
        {
            // when
            lockClient.tryExclusiveLock( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
        }
    }

    @Test
    public void shouldAcceptSharedLocks()
    {
        lockClient.acquireShared( NONE, NODE, 1 );
        lockClient.trySharedLock( NODE, 1 );
    }
}
