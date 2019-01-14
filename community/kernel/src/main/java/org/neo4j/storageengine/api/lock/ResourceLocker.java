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
package org.neo4j.storageengine.api.lock;

import java.util.Arrays;

import org.neo4j.kernel.impl.locking.LockTracer;

public interface ResourceLocker
{
    /**
     * Can be grabbed when no other client holds locks on the relevant resources. No other clients can hold locks
     * while one client holds an exclusive lock. If the lock cannot be acquired,
     * behavior is specified by the {@link WaitStrategy} for the given {@link ResourceType}.
     *
     * @param tracer
     * @param resourceType type or resource(s) to lock.
     * @param resourceIds id(s) of resources to lock. Multiple ids should be ordered consistently by all callers
     */
    void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException;

    ResourceLocker NONE = ( tracer, resourceType, resourceIds ) ->
    {
        throw new UnsupportedOperationException(
                "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString( resourceIds ) );
    };
}
