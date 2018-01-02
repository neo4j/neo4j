/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.locking;

import java.util.concurrent.locks.Lock;

public class LockWrapper implements AutoCloseable
{
    public static LockWrapper readLock( java.util.concurrent.locks.ReadWriteLock lock )
    {
        return new LockWrapper( lock.readLock() );
    }

    public static LockWrapper writeLock( java.util.concurrent.locks.ReadWriteLock lock )
    {
        return new LockWrapper( lock.writeLock() );
    }

    private java.util.concurrent.locks.Lock lock;

    public LockWrapper( java.util.concurrent.locks.Lock lock )
    {
        (this.lock = lock).lock();
    }

    @Override
    public void close()
    {
        if ( lock != null )
        {
            lock.unlock();
            lock = null;
        }
    }

    public Lock get()
    {
        return lock;
    }
}
