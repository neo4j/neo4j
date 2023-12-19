/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
