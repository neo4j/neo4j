/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
