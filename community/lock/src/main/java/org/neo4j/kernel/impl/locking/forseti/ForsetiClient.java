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
package org.neo4j.kernel.impl.locking.forseti;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Interrupted;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.collection.trackable.HeapTrackingLongIntHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.LockClientStateHolder;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.LockWaitEvent;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

// Please note. Except separate test cases for particular classes related to community locking
// see also LockingCompatibilityTestSuite test suite

/**
 * These clients act as agents against the lock manager. The clients hold and release locks.
 * <p/>
 * The Forseti client tracks which locks it already holds, and will only communicate with the global lock manager if
 * necessary. Grabbing the same lock multiple times will honor reentrancy et cetera, but the client will track in
 * local fields how many times the lock has been grabbed, such that it will only grab and release the lock once from
 * the
 * global lock manager.
 */
public class ForsetiClient implements LockManager.Client {
    private static final int MAX_SPINS = 1000;
    private static final long MULTIPLY_UNTIL_ITERATION = MAX_SPINS + 2;
    private static final int NO_CLIENT_ID = -1;
    private static final int NO_DEADLOCK_DEPTH = -1;

    /** resourceType -> lock map. These are the global lock maps, shared across all clients. */
    private final ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps;

    /**
     * The client uses this to track which locks it holds. It is solely an optimization to ensure we don't need to
     * coordinate if we grab the same lock multiple times.
     * <p/>
     * The data structure looks like:
     * Array[ resourceType -> Map( resourceId -> num locks ) ]
     */
    private final HeapTrackingLongIntHashMap[] sharedLockCounts;

    /** @see #sharedLockCounts */
    private final HeapTrackingLongIntHashMap[] exclusiveLockCounts;

    private final AtomicLong activeLockCount = new AtomicLong();

    /**
     * Time within which any particular lock should be acquired.
     *
     * @see GraphDatabaseSettings#lock_acquisition_timeout
     */
    private long lockAcquisitionTimeoutNano;

    private final SystemNanoClock clock;
    private boolean verboseDeadlocks;

    /** List of other clients this client is waiting for. */
    private final Set<ForsetiClient> waitList = ConcurrentHashMap.newKeySet();

    // To be able to close Locks.Client instance properly we should be able to do couple of things:
    //  - have a possibility to prevent new clients to come
    //  - wake up all the waiters and let them go
    //  - have a possibility to see how many clients are still using us and wait for them to finish
    // We need to do all of that to prevent a situation when a closing client will get a lock that will never be
    // closed and eventually will block other clients.
    private final LockClientStateHolder stateHolder = new LockClientStateHolder();

    /**
     * For exclusive locks, we only need a single re-usable one per client. We simply CAS this lock into whatever slots
     * we want to hold in the global lock map. We re-create it every time the client is reused in order to avoid issues where a reference
     * is found after the client is reused, causing false deadlocks.
     */
    private ExclusiveLock myExclusiveLock;

    private volatile boolean hasLocks;

    private final ReleaseExclusiveLocksAndClearSharedVisitor releaseExclusiveAndClearSharedVisitor =
            new ReleaseExclusiveLocksAndClearSharedVisitor();
    private final ReleaseSharedDontCheckExclusiveVisitor releaseSharedDontCheckExclusiveVisitor =
            new ReleaseSharedDontCheckExclusiveVisitor();

    /**
     * When we *wait* for a specific lock to be released to us, we assign it to this field. This helps us during the
     * secondary deadlock verification process, where we traverse the waiter/lock-owner dependency graph.
     */
    private volatile ForsetiLockManager.Lock waitingForLock;

    private volatile ResourceType waitingForResourceType;
    private volatile long waitingForResourceId;
    private volatile LockType waitingForLockType;

    private volatile long transactionId;
    private final long clientId;
    private volatile DeferredScopedMemoryTracker memoryTracker;
    private static final long CONCURRENT_NODE_SIZE = HeapEstimator.LONG_SIZE + HeapEstimator.HASH_MAP_NODE_SHALLOW_SIZE;
    private volatile long prepareThreadId;

    public ForsetiClient(
            ConcurrentMap<Long, ForsetiLockManager.Lock>[] lockMaps,
            SystemNanoClock clock,
            boolean verboseDeadlocks,
            long clientId) {
        this.lockMaps = lockMaps;
        this.sharedLockCounts = new HeapTrackingLongIntHashMap[lockMaps.length];
        this.exclusiveLockCounts = new HeapTrackingLongIntHashMap[lockMaps.length];
        this.clock = clock;
        this.verboseDeadlocks = verboseDeadlocks;
        this.clientId = clientId;
    }

    @Override
    public void initialize(LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config) {
        prepareThreadId = -1;
        stateHolder.reset();
        this.transactionId = transactionId;
        this.memoryTracker = new DeferredScopedMemoryTracker(requireNonNull(memoryTracker));
        this.lockAcquisitionTimeoutNano =
                config.get(GraphDatabaseSettings.lock_acquisition_timeout).toNanos();
        this.verboseDeadlocks = config.get(GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks);
        this.myExclusiveLock = new ExclusiveLock(this);
    }

    @Override
    public void acquireShared(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        hasLocks = true;
        stateHolder.incrementActiveClients(this);
        LockWaitEvent waitEvent = null;

        try {
            // Grab the global lock map we will be using
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];

            // And grab our local lock maps
            HeapTrackingLongIntHashMap heldShareLocks = getSharedLockCount(resourceType);
            HeapTrackingLongIntHashMap heldExclusiveLocks = getExclusiveLockCount(resourceType);

            for (long resourceId : resourceIds) {
                // First, check if we already hold this as a shared lock
                int heldCount = heldShareLocks.getIfAbsent(resourceId, NO_CLIENT_ID);
                if (heldCount != NO_CLIENT_ID) {
                    // We already have a lock on this, just increment our local reference counter.
                    heldShareLocks.put(resourceId, Math.incrementExact(heldCount));
                    continue;
                }

                // Second, check if we hold it as an exclusive lock
                if (heldExclusiveLocks.containsKey(resourceId)) {
                    // We already have an exclusive lock, so just leave that in place.
                    // When the exclusive lock is released, it will be automatically downgraded to a shared lock,
                    // since we bumped the share lock reference count.
                    heldShareLocks.put(resourceId, 1);
                    continue;
                }

                memoryTracker.allocateHeap(CONCURRENT_NODE_SIZE);

                // We don't hold the lock, so we need to grab it via the global lock map
                int tries = 0;
                SharedLock mySharedLock = null;
                long waitStartNano = clock.nanos();

                // Retry loop
                while (true) {
                    assertValid(waitStartNano, resourceType, resourceId);

                    // Check if there is a lock for this entity in the map
                    ForsetiLockManager.Lock existingLock = lockMap.get(resourceId);

                    // No lock
                    if (existingLock == null) {
                        // Try to create a new shared lock
                        if (mySharedLock == null) {
                            mySharedLock = new SharedLock(this);
                        }

                        if (lockMap.putIfAbsent(resourceId, mySharedLock) == null) {
                            // Success, we now hold the shared lock.
                            break;
                        } else {
                            continue;
                        }
                    }

                    // Someone holds shared lock on this entity, try and get in on that action
                    else if (existingLock instanceof SharedLock sharedLock) {
                        if (sharedLock.acquire(this)) {
                            // Success!
                            break;
                        }
                    } else if (existingLock instanceof ExclusiveLock) {
                        // Someone holds an exclusive lock on this entity
                        // We need to wait, just let the loop run.
                    } else {
                        throw new UnsupportedOperationException("Unknown lock type: " + existingLock);
                    }

                    if (waitEvent == null) {
                        waitEvent = tracer.waitForLock(SHARED, resourceType, transactionId, resourceId);
                    }
                    // And take note of who we are waiting for. This is used for deadlock detection.
                    waitFor(existingLock, resourceType, resourceId, SHARED, tries++);
                }

                // Make a local note about the fact that we now hold this lock
                activeLockCount.incrementAndGet();
                heldShareLocks.put(resourceId, 1);
            }
        } finally {
            if (waitEvent != null) {
                waitEvent.close();
                clearWaitState();
            }
            stateHolder.decrementActiveClients();
        }
    }

    private void noteWaitingForLock(
            ForsetiLockManager.Lock lock, ResourceType resourceType, long resourceId, LockType lockType) {
        waitingForResourceType = resourceType;
        waitingForResourceId = resourceId;
        waitingForLockType = lockType;
        waitingForLock = lock;
    }

    private void clearWaitState() {
        clearWaitList();
        waitingForLock = null;
        waitingForResourceId = -1;
        waitingForResourceType = null;
        waitingForLockType = null;
    }

    private HeapTrackingLongIntHashMap getSharedLockCount(ResourceType resourceType) {
        HeapTrackingLongIntHashMap sharedLockCount = sharedLockCounts[resourceType.typeId()];
        if (sharedLockCount == null) {
            sharedLockCount = HeapTrackingCollections.newLongIntMap(memoryTracker);
            sharedLockCounts[resourceType.typeId()] = sharedLockCount;
        }
        return sharedLockCount;
    }

    private HeapTrackingLongIntHashMap getExclusiveLockCount(ResourceType resourceType) {
        HeapTrackingLongIntHashMap exclusiveLockCount = exclusiveLockCounts[resourceType.typeId()];
        if (exclusiveLockCount == null) {
            exclusiveLockCount = HeapTrackingCollections.newLongIntMap(memoryTracker);
            exclusiveLockCounts[resourceType.typeId()] = exclusiveLockCount;
        }
        return exclusiveLockCount;
    }

    @Override
    public void acquireExclusive(LockTracer tracer, ResourceType resourceType, long... resourceIds) {
        hasLocks = true;
        stateHolder.incrementActiveClients(this);
        LockWaitEvent waitEvent = null;

        try {
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            HeapTrackingLongIntHashMap heldLocks = getExclusiveLockCount(resourceType);

            for (long resourceId : resourceIds) {
                int heldCount = heldLocks.getIfAbsent(resourceId, NO_CLIENT_ID);
                if (heldCount != NO_CLIENT_ID) {
                    // We already have a lock on this, just increment our local reference counter.
                    heldLocks.put(resourceId, Math.incrementExact(heldCount));
                    continue;
                }

                memoryTracker.allocateHeap(CONCURRENT_NODE_SIZE);
                // Grab the global lock
                ForsetiLockManager.Lock existingLock;
                int tries = 0;
                long waitStartNano = clock.nanos();
                boolean upgraded = false;
                while ((existingLock = lockMap.putIfAbsent(resourceId, myExclusiveLock)) != null) {
                    assertValid(waitStartNano, resourceType, resourceId);

                    if (existingLock instanceof SharedLock sharedLock) {
                        // Then we should upgrade that lock
                        if (tryUpgradeSharedToExclusive(
                                tracer, waitEvent, resourceType, lockMap, resourceId, sharedLock, waitStartNano)) {
                            upgraded = true;
                            break;
                        }
                    }

                    if (waitEvent == null) {
                        waitEvent = tracer.waitForLock(EXCLUSIVE, resourceType, transactionId, resourceId);
                    }
                    waitFor(existingLock, resourceType, resourceId, EXCLUSIVE, tries++);
                }

                if (upgraded) {
                    // return this memory in case of upgrade as shared lock already tracks it
                    memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                } else {
                    activeLockCount.incrementAndGet();
                }
                heldLocks.put(resourceId, 1);
            }
        } finally {
            if (waitEvent != null) {
                waitEvent.close();
                clearWaitState();
            }
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        hasLocks = true;
        stateHolder.incrementActiveClients(this);

        try {
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            HeapTrackingLongIntHashMap heldLocks = getExclusiveLockCount(resourceType);

            int heldCount = heldLocks.getIfAbsent(resourceId, NO_CLIENT_ID);
            if (heldCount != NO_CLIENT_ID) {
                // We already have a lock on this, just increment our local reference counter.
                heldLocks.put(resourceId, Math.incrementExact(heldCount));
                return true;
            }

            memoryTracker.allocateHeap(CONCURRENT_NODE_SIZE);
            // Grab the global lock
            ForsetiLockManager.Lock lock;
            if ((lock = lockMap.putIfAbsent(resourceId, myExclusiveLock)) != null) {
                if (lock instanceof SharedLock sharedLock
                        && getSharedLockCount(resourceType).containsKey(resourceId)) {
                    if (sharedLock.tryAcquireUpdateLock()) {
                        if (sharedLock.numberOfHolders() == 1) {
                            heldLocks.put(resourceId, 1);
                            return true;
                        } else {
                            sharedLock.releaseUpdateLock();
                            memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                            return false;
                        }
                    }
                }
                memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                return false;
            }

            activeLockCount.incrementAndGet();
            heldLocks.put(resourceId, 1);
            return true;
        } finally {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public boolean trySharedLock(ResourceType resourceType, long resourceId) {
        hasLocks = true;
        stateHolder.incrementActiveClients(this);

        try {
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resourceType.typeId()];
            HeapTrackingLongIntHashMap heldShareLocks = getSharedLockCount(resourceType);
            HeapTrackingLongIntHashMap heldExclusiveLocks = getExclusiveLockCount(resourceType);

            int heldCount = heldShareLocks.getIfAbsent(resourceId, NO_CLIENT_ID);
            if (heldCount != NO_CLIENT_ID) {
                // We already have a lock on this, just increment our local reference counter.
                heldShareLocks.put(resourceId, Math.incrementExact(heldCount));
                return true;
            }

            if (heldExclusiveLocks.containsKey(resourceId)) {
                // We already have an exclusive lock, so just leave that in place. When the exclusive lock is released,
                // it will be automatically downgraded to a shared lock, since we bumped the share lock reference count.
                heldShareLocks.put(resourceId, 1);
                return true;
            }

            memoryTracker.allocateHeap(CONCURRENT_NODE_SIZE);
            long waitStartNano = clock.nanos();
            while (true) {
                assertValid(waitStartNano, resourceType, resourceId);

                ForsetiLockManager.Lock existingLock = lockMap.get(resourceId);
                if (existingLock == null) {
                    // Try to create a new shared lock
                    if (lockMap.putIfAbsent(resourceId, new SharedLock(this)) == null) {
                        // Success!
                        break;
                    }
                } else if (existingLock instanceof SharedLock sharedLock) {
                    // Note that there is a "safe" race here where someone may be releasing the last reference to a lock
                    // and thus removing that lock instance (making it unacquirable). In this case, we allow retrying,
                    // even though this is a try-lock call.
                    if (sharedLock.acquire(this)) {
                        // Success!
                        break;
                    } else if (sharedLock.isUpdateLock()) {
                        memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                        return false;
                    }
                } else if (existingLock instanceof ExclusiveLock) {
                    memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                    return false;
                } else {
                    throw new UnsupportedOperationException("Unknown lock type: " + existingLock);
                }
            }
            activeLockCount.incrementAndGet();
            heldShareLocks.put(resourceId, 1);
            return true;
        } finally {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseShared(ResourceType resourceType, long... resourceIds) {
        stateHolder.incrementActiveClients(this);

        try {
            HeapTrackingLongIntHashMap sharedLocks = getSharedLockCount(resourceType);
            HeapTrackingLongIntHashMap exclusiveLocks = getExclusiveLockCount(resourceType);
            ConcurrentMap<Long, ForsetiLockManager.Lock> resourceTypeLocks = lockMaps[resourceType.typeId()];
            for (long resourceId : resourceIds) {
                if (releaseLocalLock(resourceType, resourceId, sharedLocks)) {
                    continue;
                }
                // Only release if we were not holding an exclusive lock as well
                if (!exclusiveLocks.containsKey(resourceId)) {
                    releaseGlobalLock(resourceTypeLocks, resourceId);
                }
            }
        } finally {
            stateHolder.decrementActiveClients();
        }
    }

    @Override
    public void releaseExclusive(ResourceType resourceType, long... resourceIds) {
        stateHolder.incrementActiveClients(this);

        try {
            ConcurrentMap<Long, ForsetiLockManager.Lock> resourceTypeLocks = lockMaps[resourceType.typeId()];
            HeapTrackingLongIntHashMap exclusiveLocks = getExclusiveLockCount(resourceType);
            HeapTrackingLongIntHashMap sharedLocks = getSharedLockCount(resourceType);
            for (long resourceId : resourceIds) {
                if (releaseLocalLock(resourceType, resourceId, exclusiveLocks)) {
                    continue;
                }

                if (sharedLocks.containsKey(resourceId)) {
                    // We are still holding a shared lock, so we will release it to be reused
                    ForsetiLockManager.Lock lock = resourceTypeLocks.get(resourceId);
                    if (lock instanceof SharedLock sharedLock) {
                        if (sharedLock.isUpdateLock()) {
                            sharedLock.releaseUpdateLock();
                        } else {
                            throw new IllegalStateException("Incorrect state of exclusive lock. Lock should be updated "
                                    + "to exclusive before attempt to release it. Lock: " + this);
                        }
                    } else {
                        // in case if current lock is exclusive we swap it to new shared lock
                        SharedLock sharedLock = new SharedLock(this);
                        resourceTypeLocks.put(resourceId, sharedLock);
                    }
                } else {
                    // we do not hold shared lock so we just releasing it
                    releaseGlobalLock(resourceTypeLocks, resourceId);
                }
            }
        } finally {
            stateHolder.decrementActiveClients();
        }
    }

    private void releaseAllClientLocks() {
        // Force the release of all locks held.
        for (int i = 0; i < exclusiveLockCounts.length; i++) {
            HeapTrackingLongIntHashMap exclusiveLocks = exclusiveLockCounts[i];
            HeapTrackingLongIntHashMap sharedLocks = sharedLockCounts[i];

            // Begin releasing exclusive locks, as we may hold both exclusive and shared locks on the same resource,
            // and so releasing exclusive locks means we can "throw away" our shared lock (which would normally have
            // been re-instated after releasing the exclusive lock).
            if (exclusiveLocks != null) {
                exclusiveLocks.forEachKey(releaseExclusiveAndClearSharedVisitor.initialize(sharedLocks, lockMaps[i]));
                exclusiveLockCounts[i] = null;
                exclusiveLocks.close();
            }

            // Then release all remaining shared locks
            if (sharedLocks != null) {
                sharedLocks.forEachKey(releaseSharedDontCheckExclusiveVisitor.initialize(lockMaps[i]));
                sharedLockCounts[i] = null;
                sharedLocks.close();
            }
        }
        activeLockCount.set(0);
    }

    @Override
    public void prepareForCommit() {
        prepareThreadId = Thread.currentThread().getId();
        stateHolder.prepare(this);
    }

    @Override
    public void stop() {
        stateHolder.incrementActiveClients(this);
        try {
            // marking client as stopped
            if (stateHolder.stopClient()) {
                // waiting for all operations to be completed
                waitForStopBeOnlyClient();
                memoryTracker
                        .stop(); // Stopping tracker to defer all released memory until we close the client, to ensure
                // thread safety
                releaseAllLocks();
            }
        } finally {
            stateHolder.decrementActiveClients();
        }
    }

    private void waitForStopBeOnlyClient() {
        while (!stateHolder.isSingleClient()) {
            parkNanos(MILLISECONDS.toNanos(10));
        }
    }

    private void waitForAllClientsToLeave() {
        while (stateHolder.hasActiveClients()) {
            parkNanos(MILLISECONDS.toNanos(10));
        }
    }

    @Override
    public void close() {
        stateHolder.closeClient();
        waitForAllClientsToLeave();
        releaseAllLocks();
        transactionId = INVALID_TRANSACTION_ID;
        memoryTracker.close();
        // This exclusive lock instance has been used for all exclusive locks held by this client for this transaction.
        // Close it to mark it not participate in deadlock detection anymore
        myExclusiveLock.close();
    }

    private void releaseAllLocks() {
        if (hasLocks) {
            releaseAllClientLocks();
            clearWaitList();
            hasLocks = false;
        }
    }

    @Override
    public long getTransactionId() {
        return transactionId;
    }

    @Override
    public Collection<ActiveLock> activeLocks() {
        // We're iterating the global map instead of the client local maps because this can be called from separate
        // threads
        var locks = new ArrayList<ActiveLock>();
        for (int typeId = 0; typeId < lockMaps.length; typeId++) {
            ResourceType resourceType = ResourceType.fromId(typeId);
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[typeId];
            if (lockMap != null) {
                lockMap.forEach((resourceId, lock) -> {
                    if (lock.isOwnedBy(this)) {
                        locks.add(new ActiveLock(resourceType, lock.type(), transactionId, resourceId));
                    }
                });
            }
        }
        return locks;
    }

    @Override
    public boolean holdsLock(long id, ResourceType resource, LockType lockType) {
        ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap = lockMaps[resource.typeId()];
        ForsetiLockManager.Lock lock = lockMap.get(id);
        if (lock == null) {
            return false;
        }
        // If we are looking for shared a lock and have the exclusive its fine because exclusive is more strict
        LockType type = lock.type(); // Check type atomically
        return lock.isOwnedBy(this) && (type == lockType || type == EXCLUSIVE);
    }

    @Override
    public long activeLockCount() {
        // We're keeping a threadsafe count instead of using the local lock maps sizes since this can be called from
        // separate threads
        return activeLockCount.get();
    }

    void copyWaitListTo(Set<ForsetiClient> other) {
        other.add(this);
        other.addAll(waitList);
    }

    boolean isWaitingFor(ForsetiClient client) {
        return client.getTransactionId() != getTransactionId() && waitList.contains(client);
    }

    boolean isWaitingFor(ForsetiLockManager.Lock lock, ResourceType resourceType, long resourceId) {
        return waitingForLock == lock && waitingForResourceType == resourceType && waitingForResourceId == resourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ForsetiClient that = (ForsetiClient) o;

        return clientId == that.clientId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(clientId);
    }

    @Override
    public String toString() {
        return String.format("ForsetiClient[transactionId=%d, clientId=%d]", transactionId, clientId);
    }

    /** Release a lock from the global pool. */
    private void releaseGlobalLock(ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap, long resourceId) {
        ForsetiLockManager.Lock lock = lockMap.get(resourceId);
        if (lock instanceof ExclusiveLock) {
            lockMap.remove(resourceId);
            memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
        } else if (lock instanceof SharedLock sharedLock && sharedLock.release(this)) {
            // We were the last to hold this lock
            lockMap.remove(resourceId);
            memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
        }
        activeLockCount.decrementAndGet();
    }

    /** Release a lock locally, and return true if we still hold more references to that lock. */
    private boolean releaseLocalLock(ResourceType type, long resourceId, HeapTrackingLongIntHashMap localLocks) {
        int lockCount = localLocks.removeKeyIfAbsent(resourceId, NO_CLIENT_ID);
        if (lockCount == NO_CLIENT_ID) {
            throw new IllegalStateException(
                    this + " cannot release lock that it does not hold: " + type + "[" + resourceId + "].");
        }

        if (lockCount > 1) {
            localLocks.put(resourceId, lockCount - 1);
            return true;
        }
        return false;
    }

    /**
     * Attempt to upgrade a share lock to an exclusive lock, grabbing the share lock if we don't hold it.
     **/
    private boolean tryUpgradeSharedToExclusive(
            LockTracer tracer,
            LockWaitEvent waitEvent,
            ResourceType resourceType,
            ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap,
            long resourceId,
            SharedLock sharedLock,
            long waitStartNano) {
        int tries = 0;
        boolean holdsSharedLock = getSharedLockCount(resourceType).containsKey(resourceId);
        if (!holdsSharedLock) {
            memoryTracker.allocateHeap(CONCURRENT_NODE_SIZE);
            // We don't hold the shared lock, we need to grab it to upgrade it to an exclusive one
            if (!sharedLock.acquire(this)) {
                memoryTracker.releaseHeap(CONCURRENT_NODE_SIZE);
                return false;
            }
            activeLockCount.incrementAndGet();

            try {
                if (tryUpgradeToExclusiveWithShareLockHeld(
                        tracer, waitEvent, resourceType, resourceId, sharedLock, tries, waitStartNano)) {
                    return true;
                } else {
                    releaseGlobalLock(lockMap, resourceId);
                    return false;
                }
            } catch (Throwable e) {
                releaseGlobalLock(lockMap, resourceId);
                throw e;
            }
        } else {
            // We do hold the shared lock, so no reason to deal with the complexity in the case above.
            return tryUpgradeToExclusiveWithShareLockHeld(
                    tracer, waitEvent, resourceType, resourceId, sharedLock, tries, waitStartNano);
        }
    }

    /** Attempt to upgrade a share lock that we hold to an exclusive lock. */
    private boolean tryUpgradeToExclusiveWithShareLockHeld(
            LockTracer tracer,
            LockWaitEvent priorEvent,
            ResourceType resourceType,
            long resourceId,
            SharedLock sharedLock,
            int tries,
            long waitStartNano) {
        if (sharedLock.tryAcquireUpdateLock()) {
            LockWaitEvent waitEvent = null;
            try {
                // Now we just wait for all clients to release the shared lock
                while (sharedLock.numberOfHolders() > 1) {
                    assertValid(waitStartNano, resourceType, resourceId);
                    if (waitEvent == null && priorEvent == null) {
                        waitEvent = tracer.waitForLock(EXCLUSIVE, resourceType, transactionId, resourceId);
                    }
                    waitFor(sharedLock, resourceType, resourceId, EXCLUSIVE, tries++);
                }

                return true;
            } catch (Throwable e) {
                sharedLock.releaseUpdateLock();
                if (e instanceof DeadlockDetectedException || e instanceof LockClientStoppedException) {
                    throw (RuntimeException) e;
                }
                var status = (e instanceof Status.HasStatus se) ? se.status() : Status.Database.Unknown;
                throw new TransactionFailureException(
                        "Failed to upgrade shared lock to exclusive: " + sharedLock, e, status);
            } finally {
                if (waitEvent != null) {
                    waitEvent.close();
                    clearWaitState();
                }
            }
        }
        return false;
    }

    private void clearWaitList() {
        waitList.clear();
    }

    private void waitFor(
            ForsetiLockManager.Lock lock, ResourceType type, long resourceId, LockType lockType, int tries) {
        clearAndCopyWaitList(lock);
        noteWaitingForLock(lock, type, resourceId, lockType);
        incrementalBackoffWait(tries);

        ForsetiClient clientId = lock.detectDeadlock(this);
        if (clientId != null && shouldAbort(clientId)) {
            // If the deadlock is real, then an owner of this lock must be (transitively) waiting on a lock that
            // we own. So to verify the deadlock, we traverse the lock owners and their `waitingForLock` fields,
            // to find a lock that has us among the owners.
            // We only act upon the result of this method if the `tries` count is above some threshold. The reason
            // is that the Lock.collectOwners, which is algorithm relies upon, is inherently racy, and so only
            // reduces the probability of a false positive, but does not eliminate them.
            int depth;
            if (tries > 100 && ((depth = isDeadlockReal(lock)) != NO_DEADLOCK_DEPTH)) {
                // After checking several times, this really does look like a real deadlock.
                if (verboseDeadlocks) {
                    var deadlockCycleMessage = findDeadlockPath(lock, type, resourceId, depth);
                    if (deadlockCycleMessage != null) {
                        var message = String.format(
                                "%s can't acquire %s %s because it would form this deadlock wait cycle:%n%s",
                                this, lockType, lockString(type, resourceId), deadlockCycleMessage);
                        throw new DeadlockDetectedException(message);
                    }
                    // else we tried to find a precise deadlock cycle, but found none - which means that
                    // there was no real deadlock
                } else {
                    throw new DeadlockDetectedException(format(
                            "%s can't acquire %s on %s because holders of that lock are waiting for %s.%n Wait list:%s",
                            this, lock, lockString(type, resourceId), this, lock.describeWaitList()));
                }
            }
            Thread.yield();
        } else if ((tries & 8191) == 8191) // Each try sleeps for up to 1ms, so 8k tries will be every ~8s
        {
            for (ForsetiClient client : waitList) {
                if (clientCommittingByCurrentThread(client) && isDeadlockReal(lock) != -1) {
                    String message = this + " can't acquire " + lock + " on " + type + "(" + resourceId
                            + "), because we are waiting for " + client + " that is committing on the same thread";
                    throw new DeadlockDetectedException(message);
                }
            }
        }
    }

    @VisibleForTesting
    public static void incrementalBackoffWait(long iteration) {
        if (iteration < MAX_SPINS) {
            Thread.onSpinWait();
            return;
        }

        try {
            if (iteration < MULTIPLY_UNTIL_ITERATION) {
                parkNanos(500);
            } else {
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new LockAcquisitionTimeoutException(Interrupted, "Interrupted while waiting.");
        }
    }

    /**
     * When a deadlock occurs, the client with the fewest number of held locks is aborted. If both clients hold the same
     * number of
     * locks, the client with the lowest client id is aborted.
     * <p/>
     * This is one side of a long academic argument, where the other says to abort the one with the most locks held,
     * since it's old and monolithic and holding up
     * the line.
     */
    private boolean shouldAbort(ForsetiClient clientWereDeadlockedWith) {
        if (getTransactionId() == clientWereDeadlockedWith.getTransactionId()) {
            return true; // same client
        }

        long ourCount = this.activeLockCount();
        long otherCount = clientWereDeadlockedWith.activeLockCount();
        if (ourCount > otherCount) {
            // We hold more locks than the other client, we stay the course!
            return false;
        } else if (otherCount > ourCount) {
            // Other client holds more locks than us, yield to her seniority
            return true;
        } else {
            return getTransactionId()
                    > clientWereDeadlockedWith
                            .getTransactionId(); // >= to guard against bugs where a client thinks its deadlocked itself
        }
    }

    private boolean clientCommittingByCurrentThread(ForsetiClient otherClient) {
        return otherClient != this
                && otherClient.stateHolder.isPrepared()
                && Thread.currentThread().getId() == otherClient.prepareThreadId;
    }

    private void clearAndCopyWaitList(ForsetiLockManager.Lock lock) {
        clearWaitList();
        lock.copyHolderWaitListsInto(waitList);
    }

    private int isDeadlockReal(ForsetiLockManager.Lock lock) {
        if (isDeadlockRealInternal(lock) != NO_DEADLOCK_DEPTH) {
            parkNanos(MILLISECONDS.toNanos(10));
            return isDeadlockRealInternal(lock);
        }
        return NO_DEADLOCK_DEPTH;
    }

    private int isDeadlockRealInternal(ForsetiLockManager.Lock lock) {
        Set<ForsetiLockManager.Lock> waitedUpon = new HashSet<>();
        Set<ForsetiClient> owners = new HashSet<>();
        Set<ForsetiLockManager.Lock> nextWaitedUpon = new HashSet<>();
        Set<ForsetiClient> nextOwners = new HashSet<>();
        lock.collectOwners(owners);

        int depth = 1;
        do {
            depth++;
            waitedUpon.addAll(nextWaitedUpon);
            boolean foundSameThreadClientCycle = collectNextOwners(waitedUpon, owners, nextWaitedUpon, nextOwners);
            if (foundSameThreadClientCycle || nextOwners.contains(this) && lock.detectDeadlock(this) != null) {
                return depth;
            }
            owners.clear();
            Set<ForsetiClient> ownersTmp = owners;
            owners = nextOwners;
            nextOwners = ownersTmp;
        } while (!nextWaitedUpon.isEmpty());
        // Nope, we didn't find any real wait cycles.
        return NO_DEADLOCK_DEPTH;
    }

    private String findDeadlockPath(
            ForsetiLockManager.Lock lockToWaitFor, ResourceType type, long resourceId, int maxDepth) {
        var parents = new ArrayList<LockPath>();
        var owners = new HashSet<ForsetiClient>();
        var paths = new ArrayList<LockPath>();
        traverseOneStep(null, lockToWaitFor, parents, 0, owners);
        for (var depth = 1; depth <= maxDepth + 1; depth++) {
            for (var parentPath : parents) {
                var lock = parentPath.ownerWaitingForLock();
                if (!lock.isClosed()) {
                    var path = traverseOneStep(parentPath, lock, paths, depth, owners);
                    if (path != null) {
                        return path.stringify(lockToWaitFor, type, resourceId);
                    }
                }
            }
            parents = paths;
            paths = new ArrayList<>();
        }
        return null;
    }

    static String lockString(ResourceType resourceType, long resourceId) {
        return format("%s(%d)", resourceType, resourceId);
    }

    private LockPath traverseOneStep(
            LockPath parentPath,
            ForsetiLockManager.Lock lock,
            List<LockPath> paths,
            int depth,
            Set<ForsetiClient> owners) {
        owners.clear();
        lock.collectOwners(owners);
        for (var owner : owners) {
            var ownerWaitingForLock = owner.waitingForLock;
            var ownerWaitingForResourceType = owner.waitingForResourceType;
            var ownerWaitingForResourceId = owner.waitingForResourceId;
            var ownerWaitingForLockType = owner.waitingForLockType;
            var ownerTransactionId = owner.transactionId;
            if (ownerWaitingForLock == null || ownerWaitingForResourceType == null || ownerWaitingForLock.isClosed()) {
                continue;
            }

            if (parentPath != null && parentPath.containsOwner(ownerTransactionId)) {
                continue;
            }

            var path = new LockPath(
                    owner,
                    ownerTransactionId,
                    ownerWaitingForLock,
                    ownerWaitingForResourceType,
                    ownerWaitingForResourceId,
                    ownerWaitingForLockType,
                    parentPath);
            if (owner == this && depth > 0) {
                // We've found a cycle
                return path;
            }
            paths.add(path);
        }
        return null;
    }

    /**
     * @return true if found a client in the lock graph that is committing by us, false otherwise
     */
    private boolean collectNextOwners(
            Set<ForsetiLockManager.Lock> waitedUpon,
            Set<ForsetiClient> owners,
            Set<ForsetiLockManager.Lock> nextWaitedUpon,
            Set<ForsetiClient> nextOwners) {
        nextWaitedUpon.clear();
        for (ForsetiClient owner : owners) {
            if (clientCommittingByCurrentThread(owner)) {
                return true;
            }
            ForsetiLockManager.Lock waitingForLock = owner.waitingForLock;
            if (waitingForLock != null && !waitingForLock.isClosed() && !waitedUpon.contains(waitingForLock)) {
                nextWaitedUpon.add(waitingForLock);
            }
        }
        for (ForsetiLockManager.Lock lck : nextWaitedUpon) {
            lck.collectOwners(nextOwners);
        }
        return false;
    }

    String describeWaitList() {
        StringBuilder sb = new StringBuilder(format("%nClient[%d] waits for [", getTransactionId()));
        var iter = waitList.iterator();
        for (boolean first = true; iter.hasNext(); ) {
            ForsetiClient next = iter.next();
            if (next.getTransactionId() == getTransactionId()) {
                // Skip our own id from the wait list, that's an implementation detail
                continue;
            }
            sb.append((!first) ? "," : "").append(next);
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private void assertValid(long waitStartNano, ResourceType resourceType, long resourceId) {
        assertNotStopped();
        assertNotExpired(waitStartNano, resourceType, resourceId);
    }

    private void assertNotStopped() {
        if (stateHolder.isStopped()) {
            throw new LockClientStoppedException(this);
        }
    }

    private void assertNotExpired(long waitStartNano, ResourceType resourceType, long resourceId) {
        long timeoutNano = this.lockAcquisitionTimeoutNano;
        if (timeoutNano > 0) {
            if ((clock.nanos() - waitStartNano) > timeoutNano) {
                throw new LockAcquisitionTimeoutException(resourceType, resourceId, timeoutNano);
            }
        }
    }

    public long transactionId() {
        return transactionId;
    }

    // Visitors used for bulk ops on the lock maps (such as releasing all locks)

    /**
     * Release all shared locks, assuming that there will be no exclusive locks held by this client, such that there
     * is no need to check for those. It is used when releasing all locks.
     */
    private class ReleaseSharedDontCheckExclusiveVisitor implements LongProcedure {
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private LongProcedure initialize(ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap) {
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public void value(long resourceId) {
            releaseGlobalLock(lockMap, resourceId);
        }
    }

    /**
     * Release exclusive locks and remove any local reference to the shared lock.
     * This is an optimization used when releasing all locks.
     */
    private class ReleaseExclusiveLocksAndClearSharedVisitor implements LongProcedure {
        private HeapTrackingLongIntHashMap sharedLockCounts;
        private ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap;

        private LongProcedure initialize(
                HeapTrackingLongIntHashMap sharedLockCounts, ConcurrentMap<Long, ForsetiLockManager.Lock> lockMap) {
            this.sharedLockCounts = sharedLockCounts;
            this.lockMap = lockMap;
            return this;
        }

        @Override
        public void value(long resourceId) {
            releaseGlobalLock(lockMap, resourceId);

            // If we hold this as a shared lock, we can throw that shared lock away directly, since we haven't
            // followed the down-grade protocol.
            if (sharedLockCounts != null) {
                sharedLockCounts.remove(resourceId);
            }
        }
    }

    /**
     * A memory tracker with possibly deferred release. It will delegate all operations until stopped, where releases will be deferred until closed.
     * Releasing memory after stopped will be done by closing the tracker.
     * The forseti client can be stopped by different threads (e.g on transaction termination),
     * causing race condition with the passed in memory tracker. This object is intended to be owned by the client,
     * where we have control over the allocations.
     */
    private static class DeferredScopedMemoryTracker extends DefaultScopedMemoryTracker {
        private boolean stopped;

        DeferredScopedMemoryTracker(MemoryTracker delegate) {
            super(delegate);
        }

        @Override
        public void releaseHeap(long bytes) {
            if (!stopped) {
                super.releaseHeap(bytes);
            }
        }

        @Override
        public void releaseNative(long bytes) {
            if (!stopped) {
                super.releaseNative(bytes);
            }
        }

        void stop() {
            stopped = true;
        }
    }
}
