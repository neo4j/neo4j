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
package org.neo4j.util.locks;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingSupplier;

/**
 * Helper for the common try-finally patten of locking.
 */
public class CriticalSection
{
    private final Lock lock;

    public CriticalSection()
    {
        this( new ReentrantLock() );
    }

    CriticalSection( Lock lock )
    {
        this.lock = lock;
    }

    public <T,E extends Exception> T lock( ThrowingSupplier<T,E> supplier ) throws E
    {
        lock.lock();
        try
        {
            return supplier.get();
        }
        finally
        {
            lock.unlock();
        }
    }

    public <E extends Exception> void lock( ThrowingAction<E> action ) throws E
    {
        lock.lock();
        try
        {
            action.apply();
        }
        finally
        {
            lock.unlock();
        }
    }
}
