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
package org.neo4j.kernel.impl.locking;

import java.util.Collection;
import java.util.Collections;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

public class NoLocksClient implements LockManager.Client {

    public static final NoLocksClient NO_LOCKS_CLIENT = new NoLocksClient();

    private NoLocksClient() {}

    @Override
    public void initialize(LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config) {}

    @Override
    public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {}

    @Override
    public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {}

    @Override
    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        return false;
    }

    @Override
    public boolean trySharedLock(ResourceType resourceType, long resourceId) {
        return false;
    }

    @Override
    public void releaseShared(ResourceType resourceType, long... resourceIds) {}

    @Override
    public void releaseExclusive(ResourceType resourceType, long... resourceIds) {}

    @Override
    public void prepareForCommit() {}

    @Override
    public void stop() {}

    @Override
    public void close() {}

    @Override
    public long getTransactionId() {
        return -1;
    }

    @Override
    public Collection<ActiveLock> activeLocks() {
        return Collections.emptyList();
    }

    @Override
    public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
        return false;
    }

    @Override
    public long activeLockCount() {
        return 0;
    }

    @Override
    public void reset() {}
}
