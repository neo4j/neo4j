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
package org.neo4j.kernel.impl.locking.multiversion;

import static org.neo4j.lock.ResourceType.PAGE;

import java.util.Collection;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

public class MultiVersionLockManager implements LockManager {
    private final LockManager storageLockManager;

    public MultiVersionLockManager(LockManager storageLockManager) {
        this.storageLockManager = storageLockManager;
    }

    @Override
    public Client newClient() {
        return new MultiversionLockClient(storageLockManager.newClient());
    }

    @Override
    public void accept(Visitor visitor) {
        storageLockManager.accept(visitor);
    }

    @Override
    public void close() {
        storageLockManager.close();
    }

    private static class MultiversionLockClient implements Client {
        private final Client delegate;

        public MultiversionLockClient(Client delegate) {
            this.delegate = delegate;
        }

        @Override
        public void initialize(
                LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config) {
            delegate.initialize(leaseClient, transactionId, memoryTracker, config);
        }

        @Override
        public boolean trySharedLock(ResourceType resourceType, long resourceId) {
            if (resourceType != PAGE) {
                return false;
            }
            return delegate.trySharedLock(resourceType, resourceId);
        }

        @Override
        public void prepareForCommit() {
            delegate.prepareForCommit();
        }

        @Override
        public void stop() {
            delegate.stop();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public long getTransactionId() {
            return delegate.getTransactionId();
        }

        @Override
        public long activeLockCount() {
            return delegate.activeLockCount();
        }

        @Override
        public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
            if (resourceType != PAGE) {
                return false;
            }
            return delegate.tryExclusiveLock(resourceType, resourceId);
        }

        @Override
        public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            if (resourceType != PAGE) {
                return;
            }
            delegate.acquireExclusive(tracer, resourceType, resourceIds);
        }

        @Override
        public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
            if (resourceType != PAGE) {
                return;
            }
            delegate.releaseExclusive(resourceType, resourceIds);
        }

        @Override
        public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
            if (resourceType != PAGE) {
                return;
            }
            delegate.acquireShared(tracer, resourceType, resourceIds);
        }

        @Override
        public void releaseShared(ResourceType resourceType, long... resourceIds) {
            if (resourceType != PAGE) {
                return;
            }
            delegate.releaseShared(resourceType, resourceIds);
        }

        @Override
        public Collection<ActiveLock> activeLocks() {
            return delegate.activeLocks();
        }

        @Override
        public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
            return delegate.holdsLock(id, resource, lockType);
        }
    }
}
