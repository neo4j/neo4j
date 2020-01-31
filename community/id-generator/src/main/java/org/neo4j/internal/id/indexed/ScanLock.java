/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.id.indexed;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock for the {@link FreeIdScanner}. Only a single thread can perform a scan at any given time,
 * but the behaviour for threads that doesn't get the lock are configurable, which is why this abstraction exists.
 */
abstract class ScanLock
{
    final ReentrantLock lock = new ReentrantLock( true );

    /**
     * Tries to acquire the lock, returning {@code true} if it succeeds, otherwise {@code false}.
     * @return {@code true} if the lock was acquired. Potentially this call blocks.
     */
    abstract boolean tryLock();

    /**
     * Releases the lock, if it was previous acquired in {@link #tryLock()}, i.e. if it returned {@code true}.
     */
    void lock()
    {
        lock.lock();
    }

    void unlock()
    {
        lock.unlock();
    }

    /**
     * Optimistic {@link ScanLock} which will never block in {@link #tryLock()}, but simply return {@code false} if it was acquire by someone else.
     * @return an optimistic and lock-free {@link ScanLock}.
     */
    static ScanLock lockFreeAndOptimistic()
    {
        return new ScanLock()
        {
            @Override
            boolean tryLock()
            {
                return lock.tryLock();
            }
        };
    }

    /**
     * Pessimistic {@link ScanLock} which may block if the lock is already acquired by someone else.
     * @return a pessimistic and blocking {@link ScanLock}.
     */
    static ScanLock lockyAndPessimistic()
    {
        return new ScanLock()
        {
            @Override
            boolean tryLock()
            {
                lock.lock();
                return true;
            }
        };
    }
}
