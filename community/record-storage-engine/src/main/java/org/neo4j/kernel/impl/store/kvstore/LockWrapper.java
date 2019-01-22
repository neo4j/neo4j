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
package org.neo4j.kernel.impl.store.kvstore;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.neo4j.logging.Logger;
import org.neo4j.util.FeatureToggles;

public class LockWrapper implements AutoCloseable
{
    private static final boolean debugLocking = FeatureToggles.flag( AbstractKeyValueStore.class, "debugLocking", false );

    public static LockWrapper readLock( UpdateLock lock, Logger logger )
    {
        return new LockWrapper( lock.readLock(), lock, logger );
    }

    public static LockWrapper writeLock( UpdateLock lock, Logger logger )
    {
        return new LockWrapper( lock.writeLock(), lock, logger );
    }

    private Lock lock;

    private LockWrapper( Lock lock, UpdateLock managingLock, Logger logger )
    {
        this.lock = lock;
        if ( debugLocking )
        {
            if ( !lock.tryLock() )
            {
                logger.log( Thread.currentThread() + " may block on " + lock + " of " + managingLock );
                while ( !tryLockBlocking( lock, managingLock, logger ) )
                {
                    logger.log( Thread.currentThread() + " still blocked on " + lock + " of " + managingLock );
                }
            }
        }
        else
        {
            lock.lock();
        }
    }

    private static boolean tryLockBlocking( Lock lock, UpdateLock managingLock, Logger logger )
    {
        try
        {
            return lock.tryLock( 1, TimeUnit.HOURS );
        }
        catch ( InterruptedException e )
        {
            logger.log( Thread.currentThread() + " ignoring interrupt while blocked on " + lock + " of " + managingLock );
        }
        return false;
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
