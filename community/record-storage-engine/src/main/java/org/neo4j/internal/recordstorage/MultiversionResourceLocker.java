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
package org.neo4j.internal.recordstorage;

import static org.neo4j.kernel.impl.store.RecordPageLocationCalculator.pageIdForRecord;

import java.util.Collection;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;

public class MultiversionResourceLocker implements ResourceLocker {
    public static final int PAGE_ID_BITS = 54;
    public static final long PAGE_ID_MASK = (1L << PAGE_ID_BITS) - 1;

    private final ResourceLocker locks;
    private final int recordsPerPage;

    public MultiversionResourceLocker(ResourceLocker locks, RelationshipStore relationshipStore) {
        this.locks = locks;
        this.recordsPerPage = relationshipStore.getRecordsPerPage();
    }

    @Override
    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        if (resourceType != ResourceType.RELATIONSHIP) {
            return locks.tryExclusiveLock(resourceType, resourceId);
        }
        return locks.tryExclusiveLock(ResourceType.PAGE, getResourceId(resourceId));
    }

    private long getResourceId(long resourceId) {
        return pageIdForRecord(resourceId, recordsPerPage) | ((long) StoreType.RELATIONSHIP.ordinal() << PAGE_ID_BITS);
    }

    @Override
    public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        if (resourceType != ResourceType.RELATIONSHIP) {
            locks.acquireExclusive(tracer, resourceType, resourceIds);
        } else if (resourceIds.length == 1) {
            locks.acquireExclusive(tracer, resourceType, getResourceId(resourceIds[0]));
        } else {
            long[] pageIds = new long[resourceIds.length];
            for (int i = 0; i < resourceIds.length; i++) {
                pageIds[i] = pageIdForRecord(resourceIds[i], recordsPerPage);
            }
            locks.acquireExclusive(tracer, resourceType, pageIds);
        }
    }

    @Override
    public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
        if (resourceType != ResourceType.RELATIONSHIP) {
            locks.releaseExclusive(resourceType, resourceIds);
        } else if (resourceIds.length == 1) {
            locks.releaseExclusive(resourceType, pageIdForRecord(resourceIds[0], recordsPerPage));
        } else {
            long[] pageIds = new long[resourceIds.length];
            for (int i = 0; i < resourceIds.length; i++) {
                pageIds[i] = pageIdForRecord(resourceIds[i], recordsPerPage);
            }
            locks.releaseExclusive(resourceType, pageIds);
        }
    }

    @Override
    public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        if (resourceType != ResourceType.RELATIONSHIP) {
            locks.acquireShared(tracer, resourceType, resourceIds);
        } else if (resourceIds.length == 1) {
            locks.acquireShared(tracer, resourceType, getResourceId(resourceIds[0]));
        } else {
            long[] pageIds = new long[resourceIds.length];
            for (int i = 0; i < resourceIds.length; i++) {
                pageIds[i] = pageIdForRecord(resourceIds[i], recordsPerPage);
            }
            locks.acquireShared(tracer, resourceType, pageIds);
        }
    }

    @Override
    public void releaseShared(ResourceType resourceType, long... resourceIds) {
        if (resourceType != ResourceType.RELATIONSHIP) {
            locks.releaseShared(resourceType, resourceIds);
        } else if (resourceIds.length == 1) {
            locks.releaseShared(resourceType, getResourceId(resourceIds[0]));
        } else {
            long[] pageIds = new long[resourceIds.length];
            for (int i = 0; i < resourceIds.length; i++) {
                pageIds[i] = pageIdForRecord(resourceIds[i], recordsPerPage);
            }
            locks.releaseShared(resourceType, pageIds);
        }
    }

    @Override
    public Collection<ActiveLock> activeLocks() {
        return locks.activeLocks();
    }

    @Override
    public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
        return locks.holdsLock(id, resource, lockType);
    }
}
