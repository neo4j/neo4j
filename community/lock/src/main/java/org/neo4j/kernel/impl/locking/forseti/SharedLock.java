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

import static java.lang.invoke.MethodHandles.lookup;
import static org.neo4j.internal.helpers.VarHandleUtils.consumeInt;
import static org.neo4j.internal.helpers.VarHandleUtils.getVarHandle;

import java.lang.invoke.VarHandle;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.lock.LockType;

/**
 * A Forseti share lock. Can be upgraded to an update lock, which will block new attempts at acquiring shared lock,
 * but will allow existing holders to complete.
 */
class SharedLock implements ForsetiLockManager.Lock {
    /**
     * The update lock flag is inlined into the ref count integer, in order to allow common CAS operations across
     * both the update flag and the refCount simultaneously. This avoids a nasty series of race conditions, but
     * makes the reference counting code much mode complicated. May be worth revisiting.
     */
    private static final int UPDATE_LOCK_FLAG = 1 << 31;

    @SuppressWarnings("FieldMayBeFinal")
    private volatile int refCount = 1;

    private static final VarHandle REF_COUNT = getVarHandle(lookup(), "refCount");

    private final ConcurrentHashMap.KeySetView<ForsetiClient, Boolean> clientsHoldingThisLock =
            ConcurrentHashMap.newKeySet();

    SharedLock(ForsetiClient client) {
        clientsHoldingThisLock.add(client);
    }

    public boolean acquire(ForsetiClient client) {
        // First, bump refcount to make sure no one drops this lock on the floor
        if (!acquireReference()) {
            return false;
        }

        // Then add our wait list to the pile of things waiting in case if we are not there yet
        // if we are already waiting we will release a reference to keep counter in sync
        if (clientsHoldingThisLock.add(client)) {
            return true;
        } else {
            releaseReference();
            return false;
        }
    }

    public boolean release(ForsetiClient client) {
        removeClientHoldingLock(client);
        return releaseReference();
    }

    @Override
    public void copyHolderWaitListsInto(Set<ForsetiClient> waitList) {
        for (ForsetiClient client : clientsHoldingThisLock) {
            client.copyWaitListTo(waitList);
        }
    }

    @Override
    public ForsetiClient detectDeadlock(ForsetiClient clientId) {
        if (!isClosed()) {
            for (ForsetiClient client : clientsHoldingThisLock) {
                if (client.isWaitingFor(clientId)) {
                    return client;
                }
            }
        }
        return null;
    }

    /**
     * Try to promote to an exclusive lock. You have to be holding the shared lock already.
     * @return {@code true} if we got it, {@code false} otherwise
     */
    boolean tryAcquireUpdateLock() {
        return ((int) REF_COUNT.getAndBitwiseOr(this, UPDATE_LOCK_FLAG) & UPDATE_LOCK_FLAG) == 0;
    }

    void releaseUpdateLock() {
        consumeInt((int) REF_COUNT.getAndBitwiseAnd(this, ~UPDATE_LOCK_FLAG));
    }

    int numberOfHolders() {
        return refCount & ~UPDATE_LOCK_FLAG;
    }

    boolean isUpdateLock() {
        return (refCount & UPDATE_LOCK_FLAG) == UPDATE_LOCK_FLAG;
    }

    @Override
    public String describeWaitList() {
        return clientsHoldingThisLock.stream()
                .map(ForsetiClient::describeWaitList)
                .collect(Collectors.joining(", ", "SharedLock[", "]"));
    }

    @Override
    public void collectOwners(Set<ForsetiClient> owners) {
        owners.addAll(clientsHoldingThisLock);
    }

    @Override
    public boolean isOwnedBy(ForsetiClient client) {
        return clientsHoldingThisLock.contains(client);
    }

    @Override
    public LockType type() {
        return isUpdateLock() && numberOfHolders() <= 1 ? LockType.EXCLUSIVE : LockType.SHARED;
    }

    @Override
    public LongSet transactionIds() {
        Set<ForsetiClient> lockClients = new HashSet<>();
        collectOwners(lockClients);
        return LongSets.immutable.ofAll(lockClients.stream().mapToLong(ForsetiClient::transactionId));
    }

    @Override
    public boolean isClosed() {
        return numberOfHolders() == 0;
    }

    @Override
    public String toString() {
        StringBuilder owners = new StringBuilder();
        for (ForsetiClient forsetiClient : clientsHoldingThisLock) {
            if (owners.length() > 0) {
                owners.append(", ");
            }
            owners.append(forsetiClient);
        }
        String specificLockType;
        int refCount;
        if (isUpdateLock()) {
            specificLockType = "UpdateLock";
            refCount = numberOfHolders();
        } else {
            specificLockType = "SharedLock";
            refCount = this.refCount;
        }

        return String.format("%s{owners=%s, refCount=%d}", specificLockType, owners, refCount);
    }

    private void removeClientHoldingLock(ForsetiClient client) {
        if (!clientsHoldingThisLock.remove(client)) {
            throw new IllegalStateException(
                    client + " asked to be removed from holder list, but it does not hold " + this);
        }
    }

    private boolean acquireReference() {
        while (true) {
            int refs = refCount;
            // UPDATE_LOCK flips the sign bit, so refs will be < 0 if it is an update lock.
            if (refs > 0) {
                if (REF_COUNT.weakCompareAndSet(this, refs, refs + 1)) {
                    return true;
                }
            } else {
                return false;
            }
        }
    }

    private boolean releaseReference() {
        while (true) {
            int refAndUpdateFlag = refCount;
            int newRefCount = (refAndUpdateFlag & ~UPDATE_LOCK_FLAG) - 1;
            if (REF_COUNT.weakCompareAndSet(
                    this, refAndUpdateFlag, newRefCount | (refAndUpdateFlag & UPDATE_LOCK_FLAG))) {
                return newRefCount == 0;
            }
        }
    }
}
