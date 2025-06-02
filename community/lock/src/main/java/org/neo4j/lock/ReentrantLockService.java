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

import static java.util.concurrent.locks.LockSupport.parkNanos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.neo4j.util.VisibleForTesting;

/**
 * Uses {@link ReentrantReadWriteLock} internally for locking.
 */
public class ReentrantLockService implements LockService {
    private final ConcurrentMap<LockedEntity, LockInstance> locks = new ConcurrentHashMap<>();

    @VisibleForTesting
    int lockCount() {
        return locks.size();
    }

    @Override
    public Lock acquireNodeLock(long nodeId, LockType type) {
        return acquire(new LockedNode(nodeId), type);
    }

    @Override
    public Lock acquireRelationshipLock(long relationshipId, LockType type) {
        return acquire(new LockedRelationship(relationshipId), type);
    }

    @Override
    public Lock acquireCustomLock(int resourceType, long id, LockType type) {
        return acquire(new CustomLockedEntity(resourceType, id), type);
    }

    private Lock acquire(LockedEntity key, LockType type) {
        var lockInstance = lockInstance(key);
        var variant = lockInstance.acquire(type);
        return new Lock() {
            private boolean released;

            @Override
            public void release() {
                if (!released) {
                    variant.unlock();
                    if (lockInstance.deref()) {
                        locks.remove(key);
                    }
                    released = true;
                }
            }

            @Override
            public String toString() {
                StringBuilder repr =
                        new StringBuilder("{").append(key.toString()).append(' ');
                if (released) {
                    repr.append("RELEASED");
                } else {
                    repr.append(lockInstance);
                }
                return repr.append('}').toString();
            }
        };
    }

    private LockInstance lockInstance(LockedEntity key) {
        LockInstance suggestion = new LockInstance();
        while (true) {
            var lockInstance = locks.putIfAbsent(key, suggestion);
            if (lockInstance == null) {
                return suggestion;
            }

            if (lockInstance.ref()) {
                return lockInstance;
            }
            parkNanos(1_000_000);
        }
    }

    private sealed interface LockedEntity {}

    private record LockedNode(long id) implements LockedEntity {}

    private record LockedRelationship(long id) implements LockedEntity {}

    private record CustomLockedEntity(int type, long id) implements LockedEntity {}

    private record LockInstance(ReentrantReadWriteLock lock, AtomicInteger usage) {
        private static final int DEAD = -1;

        LockInstance() {
            this(new ReentrantReadWriteLock(true), new AtomicInteger(1));
        }

        boolean ref() {
            return usage.updateAndGet(operand -> operand == DEAD ? DEAD : operand + 1) != DEAD;
        }

        java.util.concurrent.locks.Lock acquire(LockType type) {
            var variant =
                    switch (type) {
                        case EXCLUSIVE -> lock.writeLock();
                        case SHARED -> lock.readLock();
                    };
            variant.lock();
            return variant;
        }

        boolean deref() {
            return usage.updateAndGet(operand -> {
                        assert operand > 0;
                        int result = operand - 1;
                        return result == 0 ? DEAD : result;
                    })
                    == DEAD;
        }

        @Override
        public String toString() {
            return lock.toString();
        }
    }
}
