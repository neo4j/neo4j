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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;
import static org.neo4j.lock.ResourceType.RELATIONSHIP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.bag.sorted.MutableSortedBag;
import org.eclipse.collections.api.factory.SortedBags;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.test.RandomSupport;

/**
 * {@link ResourceLocker} that keeps track of acquired locks so that they can be verified later after test.
 * It can also be configured with a chance of getting a lock in {@link #tryExclusiveLock(ResourceType, long)}, this to mimic
 * some sort of concurrency without actually having multiple threads.
 */
class TrackingResourceLocker implements ResourceLocker {
    private final Set<ResourceType> strictAssertions = new HashSet<>();
    private final HashMap<ResourceType, MutableSortedBag<Long>> exclusiveLocks = new HashMap<>();
    private final HashMap<ResourceType, MutableSortedBag<Long>> sharedLocks = new HashMap<>();
    private final RandomSupport random;
    private final LockAcquisitionMonitor lockAcquisitionMonitor;
    private final int changeOfGettingTryLock;
    private boolean isPreModify;

    TrackingResourceLocker(RandomSupport random, LockAcquisitionMonitor lockAcquisitionMonitor) {
        this(random, lockAcquisitionMonitor, random.nextInt(10, 90) /*%*/);
    }

    TrackingResourceLocker(
            RandomSupport random, LockAcquisitionMonitor lockAcquisitionMonitor, int chanceOfGettingTrylockInPercent) {
        assert chanceOfGettingTrylockInPercent >= 0 && chanceOfGettingTrylockInPercent <= 100
                : chanceOfGettingTrylockInPercent;
        this.random = random;
        this.lockAcquisitionMonitor = lockAcquisitionMonitor;
        this.changeOfGettingTryLock = chanceOfGettingTrylockInPercent;
    }

    TrackingResourceLocker withStrictAssertionsOn(ResourceType resourceType) {
        strictAssertions.add(resourceType);
        return this;
    }

    void preModify(boolean isPreModify) {
        this.isPreModify = isPreModify;
    }

    @Override
    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        MutableSortedBag<Long> locks = locks(resourceType, exclusiveLocks);
        boolean hasLock = hasLock(resourceType, EXCLUSIVE, resourceId);
        boolean available = hasLock || random.nextInt(100) < changeOfGettingTryLock;
        if (available) {
            locks.add(resourceId);
            lockAcquisitionMonitor.lockAcquired(resourceType, EXCLUSIVE, resourceId, true);
        }
        return available;
    }

    @Override
    public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        acquireLock(resourceType, resourceIds, EXCLUSIVE);
    }

    private void acquireLock(ResourceType resourceType, long[] resourceIds, LockType lockType) {
        if (isPreModify && resourceType == RELATIONSHIP) {
            return;
        }

        MutableSortedBag<Long> locks = locks(resourceType, locksMap(lockType));
        boolean doStrictAssertions = strictAssertions.contains(resourceType);
        for (long id : resourceIds) {
            if (doStrictAssertions) {
                assertThat(locks.getLastOptional().orElse(-1L)).isLessThanOrEqualTo(id); // sorted
            }
            boolean added = locks.add(id);
            if (doStrictAssertions) {
                assertThat(added).isTrue();
            }
            lockAcquisitionMonitor.lockAcquired(resourceType, lockType, id, false);
        }
    }

    @Override
    public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
        releaseLock(resourceType, resourceIds, EXCLUSIVE);
    }

    private void releaseLock(ResourceType resourceType, long[] resourceIds, LockType lockType) {
        MutableSortedBag<Long> locks = locks(resourceType, locksMap(lockType));
        for (long id : resourceIds) {
            boolean removed = locks.remove(id);
            assertThat(removed).isTrue(); // should not unlock if not locked
            lockAcquisitionMonitor.lockReleased(resourceType, lockType, id, !locks.contains(id));
        }
    }

    @Override
    public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        acquireLock(resourceType, resourceIds, SHARED);
    }

    @Override
    public void releaseShared(ResourceType resourceType, long... resourceIds) {
        releaseLock(resourceType, resourceIds, SHARED);
    }

    @Override
    public Collection<ActiveLock> activeLocks() {
        List<ActiveLock> locks = new ArrayList<>();
        gatherActiveLocks(locks, exclusiveLocks, EXCLUSIVE);
        gatherActiveLocks(locks, sharedLocks, LockType.SHARED);
        return locks;
    }

    @Override
    public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
        return hasLock(resource, lockType, id) || (lockType == SHARED && hasLock(resource, EXCLUSIVE, id));
    }

    private static void gatherActiveLocks(
            List<ActiveLock> locks, HashMap<ResourceType, MutableSortedBag<Long>> locksByType, LockType lockType) {
        locksByType.forEach((resourceType, resourceIds) ->
                resourceIds.forEach(resourceId -> locks.add(new ActiveLock(resourceType, lockType, -1, resourceId))));
    }

    private static MutableSortedBag<Long> locks(
            ResourceType resourceType, HashMap<ResourceType, MutableSortedBag<Long>> locksByType) {
        return locksByType.computeIfAbsent(resourceType, type -> SortedBags.mutable.empty());
    }

    LongSet getExclusiveLocks(ResourceType resourceType) {
        return LongSets.immutable.of(
                ArrayUtils.toPrimitive(locks(resourceType, exclusiveLocks).toArray(new Long[0])));
    }

    boolean hasLock(ResourceType resourceType, LockType lockType, long resourceId) {
        return locks(resourceType, locksMap(lockType)).contains(resourceId);
    }

    boolean hasLock(ResourceType resourceType, long resourceId) {
        return locks(resourceType, locksMap(EXCLUSIVE)).contains(resourceId)
                || locks(resourceType, locksMap(SHARED)).contains(resourceId);
    }

    void assertHasLock(ResourceType resourceType, LockType lockType, long resourceId) {
        assertThat(hasLock(resourceType, lockType, resourceId))
                .as("Lock[%s,%s,%d]", resourceType, lockType, resourceId)
                .isTrue();
    }

    void assertNoLock(ResourceType resourceType, LockType lockType, long resourceId) {
        assertThat(hasLock(resourceType, lockType, resourceId))
                .as("Lock[%s,%s,%d]", resourceType, lockType, resourceId)
                .isFalse();
    }

    private HashMap<ResourceType, MutableSortedBag<Long>> locksMap(LockType lockType) {
        return switch (lockType) {
            case EXCLUSIVE -> exclusiveLocks;
            case SHARED -> sharedLocks;
        };
    }

    ResourceLocker sortOfReadOnlyView() {
        TrackingResourceLocker actual = this;
        return new ResourceLocker() {
            @Override
            public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
                return !actual.hasLock(resourceType, resourceId);
            }

            @Override
            public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
                for (long resourceId : resourceIds) {
                    if (actual.hasLock(resourceType, resourceId)) {
                        throw new AlreadyLockedException(
                                resourceType + " " + resourceId + " is locked by someone else");
                    }
                }
            }

            @Override
            public void releaseExclusive(ResourceType resourceType, long... resourceIds) {}

            @Override
            public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
                for (long resourceId : resourceIds) {
                    if (actual.hasLock(resourceType, EXCLUSIVE, resourceId)) {
                        throw new AlreadyLockedException(
                                resourceType + " " + EXCLUSIVE + " " + resourceId + " is locked by someone else");
                    }
                }
            }

            @Override
            public void releaseShared(ResourceType resourceType, long... resourceIds) {}

            @Override
            public Collection<ActiveLock> activeLocks() {
                return actual.activeLocks();
            }

            @Override
            public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
                return actual.holdsLock(id, resource, lockType);
            }
        };
    }

    public interface LockAcquisitionMonitor {
        void lockAcquired(ResourceType resourceType, LockType lockType, long resourceId, boolean tryLocked);

        void lockReleased(ResourceType resourceType, LockType lockType, long resourceId, boolean wasTheLastOne);

        LockAcquisitionMonitor NO_MONITOR = new LockAcquisitionMonitor() {
            @Override
            public void lockAcquired(
                    ResourceType resourceType, LockType lockType, long resourceId, boolean tryLocked) {}

            @Override
            public void lockReleased(
                    ResourceType resourceType, LockType lockType, long resourceId, boolean wasTheLastOne) {}
        };
    }

    static class AlreadyLockedException extends RuntimeException {
        AlreadyLockedException(String message) {
            super(message);
        }
    }
}
