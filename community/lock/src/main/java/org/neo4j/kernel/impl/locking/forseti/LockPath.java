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

import static org.neo4j.kernel.impl.locking.forseti.ForsetiClient.lockString;

import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

record LockPath(
        ForsetiClient owner,
        long ownerTransactionId,
        ForsetiLockManager.Lock ownerWaitingForLock,
        ResourceType ownerWaitingForResourceType,
        long ownerWaitingForResourceId,
        LockType ownerWaitingForLockType,
        LockPath parent) {

    boolean containsOwner(long otherOwnerTransactionId) {
        var path = this;
        while (path != null) {
            if (path.ownerTransactionId == otherOwnerTransactionId) {
                return true;
            }
            path = path.parent;
        }
        return false;
    }

    private LockPath reverse() {
        LockPath reversed = null;
        var path = this;
        while (path != null) {
            reversed = new LockPath(
                    path.owner,
                    path.ownerTransactionId,
                    path.ownerWaitingForLock,
                    path.ownerWaitingForResourceType,
                    path.ownerWaitingForResourceId,
                    path.ownerWaitingForLockType,
                    reversed);
            path = path.parent;
        }
        return reversed;
    }

    String stringify(ForsetiLockManager.Lock lock, ResourceType resourceType, long resourceId) {
        var builder = new StringBuilder();
        var path = reverse();
        var lockType = lock.type();

        builder.append(lockString(resourceType, resourceId));
        while (path != null) {
            if (path.owner.transactionId() != path.ownerTransactionId
                    || !path.owner.isWaitingFor(
                            path.ownerWaitingForLock,
                            path.ownerWaitingForResourceType,
                            path.ownerWaitingForResourceId)) {
                return null;
            }
            builder.append(String.format(
                    "-[%s_OWNER]->(tx:%d)-[WAITING_FOR_%s]->(%s)",
                    lockType,
                    path.ownerTransactionId,
                    path.ownerWaitingForLockType,
                    lockString(path.ownerWaitingForResourceType, path.ownerWaitingForResourceId)));
            lockType = path.ownerWaitingForLock.type();
            path = path.parent;
        }
        return builder.toString();
    }
}
