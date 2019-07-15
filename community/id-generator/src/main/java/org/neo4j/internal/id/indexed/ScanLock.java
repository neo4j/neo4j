/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock for the {@link FreeIdScanner}. Only a single thread can perform a scan at any given time,
 * but the behaviour for threads that doesn't get the lock are configurable, which is why this abstraction exists.
 */
abstract class ScanLock
{
    /**
     * Tries to acquire the lock, returning {@code true} if it succeeds, otherwise {@code false}.
     * @return {@code true} if the lock was acquired. Potentially this call blocks.
     */
    abstract boolean tryLock();

    abstract void lock();

    /**
     * Releases the lock, if it was previous acquired in {@link #tryLock()}, i.e. if it returned {@code true}.
     */
    abstract void unlock();

    /**
     * Implemented with a simple {@link AtomicBoolean}. Will not block if the lock is already acquired.
     * @return an optimistic and lock-free {@link ScanLock}.
     */
    static ScanLock lockFreeAndOptimistic()
    {
        return new ScanLock()
        {
            private final AtomicBoolean lockState = new AtomicBoolean();

            @Override
            boolean tryLock()
            {
                return lockState.compareAndSet( false, true );
            }

            @Override
            void lock()
            {
                while ( !tryLock() )
                {
                    try
                    {
                        Thread.sleep( 10 );
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException( e );
                    }
                }
            }

            @Override
            void unlock()
            {
                boolean unlocked = lockState.compareAndSet( true, false );
                if ( !unlocked )
                {
                    throw new IllegalStateException( "Call to unlock was made on an unlocked instance" );
                }
            }
        };
    }

    /**
     * Implemented with a {@link ReentrantLock}. {@link #tryLock() Locking} may block if the lock is already acquired by another thread,
     * but will only return {@code true} if the lock could be acquired with {@link Lock#tryLock()}, otherwise block until the lock
     * has been released and return {@code false}.
     * @return a pessimistic and blocking {@link ScanLock}.
     */
    static ScanLock lockyAndPessimistic()
    {
        return new ScanLock()
        {
            private final ReentrantLock lock = new ReentrantLock();

            @Override
            boolean tryLock()
            {
                // The idea is that either we get the lock right away, and then this is our queue to do scanning
                if ( lock.tryLock() )
                {
                    return true;
                }

                // Or someone else has it and we just wait for that someone to be done and not do scanning on our own
                lock.lock();
                lock.unlock();
                return false;
            }

            @Override
            void lock()
            {
                lock.lock();
            }

            @Override
            void unlock()
            {
                lock.unlock();
            }
        };
    }
}
