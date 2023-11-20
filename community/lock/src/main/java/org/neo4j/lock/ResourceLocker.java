/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.lock;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;

/**
 * A {@link ResourceLocker} can acquire and release both exclusive and shared locks.
 * A lock is defined by its {@link ResourceType} and id
 */
public interface ResourceLocker {
    /**
     * Tries to exclusively lock the given resource if it isn't currently locks.
     * @param resourceType type or resource to lock.
     * @param resourceId id of resources to lock.
     * @return {@code true} if the resource was locked as part of this call, otherwise {@code false} and will return without blocking.
     */
    boolean tryExclusiveLock(ResourceType resourceType, long resourceId);

    /**
     * Can be grabbed when no other client holds locks on the relevant resources. No other clients can hold locks
     * while one client holds an exclusive lock.
     *
     * @param tracer lock tracer
     * @param resourceType type or resource(s) to lock.
     * @param resourceIds id(s) of resources to lock. Multiple ids should be ordered consistently by all callers
     *
     * @throws LockAcquisitionTimeoutException in case of timeout
     */
    void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds);

    /**
     * Releases previously {@link #acquireExclusive(LockTracer, ResourceType, long...) acquired} exclusive locks.
     * @param resourceType type or resource(s) to unlock.
     * @param resourceIds id(s) of resources to unlock. Multiple ids should be ordered consistently by all callers
     */
    void releaseExclusive(ResourceType resourceType, long... resourceIds);

    /**
     * Can be grabbed when there are no locks or only share locks on a resource.
     *
     * @param tracer a tracer for listening on lock events.
     * @param resourceType type or resource(s) to lock.
     * @param resourceIds id(s) of resources to lock. Multiple ids should be ordered consistently by all callers
     */
    void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds);

    /**
     * Releases previously {@link #acquireShared(LockTracer, ResourceType, long...) acquired} shared locks.
     * @param resourceType type or resource(s) to unlock.
     * @param resourceIds id(s) of resources to unlock. Multiple ids should be ordered consistently by all callers
     */
    void releaseShared(ResourceType resourceType, long... resourceIds);

    /**
     * @return all locks that are "active", i.e. either locked or being awaited to be locked.
     */
    Collection<ActiveLock> activeLocks();

    /**
     * Checks whether or not this client currently owns the given lock.
     *
     * @param id the resource id of the lock.
     * @param resource the resource type of the lock.
     * @param lockType the type of lock.
     * @return {@code true} if this client owns the given lock, this also includes returning {@code true} if the requested
     * {@link LockType#SHARED} and this clients owns the {@link LockType#EXCLUSIVE}. Otherwise {@code false}.
     */
    boolean holdsLock(long id, ResourceType resource, LockType lockType);

    ResourceLocker PREVENT = new ResourceLocker() {
        @Override
        public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + resourceId);
        }

        @Override
        public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString(resourceIds));
        }

        @Override
        public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString(resourceIds));
        }

        @Override
        public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString(resourceIds));
        }

        @Override
        public void releaseShared(ResourceType resourceType, long... resourceIds) {
            throw new UnsupportedOperationException(
                    "Unexpected call to lock a resource " + resourceType + " " + Arrays.toString(resourceIds));
        }

        @Override
        public Collection<ActiveLock> activeLocks() {
            return Collections.emptyList();
        }

        @Override
        public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
            return false;
        }
    };

    ResourceLocker IGNORE = new ResourceLocker() {
        @Override
        public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
            return true;
        }

        @Override
        public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {}

        @Override
        public void releaseExclusive(ResourceType resourceType, long... resourceIds) {}

        @Override
        public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {}

        @Override
        public void releaseShared(ResourceType resourceType, long... resourceIds) {}

        @Override
        public Collection<ActiveLock> activeLocks() {
            return Collections.emptyList();
        }

        @Override
        public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
            return false;
        }
    };
}
