/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.lock;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A {@link ResourceLocker} can acquire and release both exclusive and shared locks.
 * A lock is defined by its {@link ResourceType} and id
 */
public interface ResourceLocker
{
    boolean tryExclusiveLock( ResourceType resourceType, long resourceId );

    /**
     * Can be grabbed when no other client holds locks on the relevant resources. No other clients can hold locks
     * while one client holds an exclusive lock. If the lock cannot be acquired,
     * behavior is specified by the {@link WaitStrategy} for the given {@link ResourceType}.
     *
     * @param tracer lock tracer
     * @param resourceType type or resource(s) to lock.
     * @param resourceIds id(s) of resources to lock. Multiple ids should be ordered consistently by all callers
     *
     * @throws AcquireLockTimeoutException in case of timeout
     */
    void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds );

    void releaseExclusive( ResourceType resourceType, long... resourceIds );

    void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds );

    void releaseShared( ResourceType resourceType, long... resourceIds );

    Stream<ActiveLock> activeLocks();

    ResourceLocker PREVENT = new ResourceLocker()
    {
        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + resourceId );
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
        {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString( resourceIds ) );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString( resourceIds ) );
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds )
        {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString( resourceIds ) );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString( resourceIds ) );
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return Stream.empty();
        }
    };

    ResourceLocker IGNORE = new ResourceLocker()
    {
        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            return true;
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return Stream.empty();
        }
    };
}
