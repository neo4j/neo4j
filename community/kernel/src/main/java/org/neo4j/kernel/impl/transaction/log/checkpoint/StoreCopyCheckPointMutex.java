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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Resource;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Mutex between {@link #storeCopy(ThrowingAction) store-copy} and {@link #checkPoint() check-point}.
 * This to prevent those two running concurrently.
 * <p>
 * Normally a store-copy implies first doing a check-point and so this relationships is somewhat intricate.
 * In addition to having {@link #storeCopy(ThrowingAction)} as the "read lock" and {@link #checkPoint()} as the
 * "write lock", {@link #storeCopy(ThrowingAction)} also accepts a code snippet to run before first concurrent
 * store-copy grabs the lock, a snippet which can include a check-point.
 *
 * <pre>
 *                                  <-WAIT--------------------|-CHECKPOINT--------->
 *                                  |                                              |
 * |----------|-----|---------------|---|-----------|---------|-------------|------|--------------------------|-> TIME
 *            |     |                   |           |         |             |                                 |
 *            |     |                   <-----STORE-|-COPY---->             <-WAIT-|-CHECKPOINT-|-STORE-COPY-->
 *            |     |                               |
 *            |     <-WAIT--|-STORE-COPY------------>
 *            |                                   |
 *            <-CHECKPOINT--|-STORE-COPY---------->
 * </pre>
 *
 * In the image above there are three "events":
 * <ol>
 * <li>Store-copy 1, where there are three concurrent store-copies going on.
 * Only the first one performs check-point</li>
 * <li>External check-point, which waits for the ongoing store-copies to complete and then performs it</li>
 * <li>Store-copy 2, which waits for the external check-point to complete and then starts its own
 * check-point, which is part of the store-copy algorithm to then perform the store-copy.</li>
 * </ol>
 *
 * Status changes are made in synchronized as opposed to atomic CAS operations, since this results
 * in simpler code and since this mutex is normally called a couple of times per hour it's not an issue.
 */
public class StoreCopyCheckPointMutex
{
    /**
     * Main lock. Read-lock is for {@link #storeCopy(ThrowingAction)} and write-lock is for {@link #checkPoint()}.
     */
    private final ReadWriteLock lock;

    /**
     * Number of currently ongoing store-copy requests.
     */
    private int storeCopyCount;

    /**
     * Whether or not the first (of the concurrently ongoing store-copy requests) has had its "before"
     * action completed. The other store-copy requests will wait for this flag to be {@code true}.
     */
    private volatile boolean storeCopyActionCompleted;

    /**
     * Error which may have happened during first concurrent store-copy request. Made available to
     * the other concurrent store-copy requests so that they can fail instead of waiting forever.
     */
    private volatile Throwable storeCopyActionError;

    public StoreCopyCheckPointMutex()
    {
        this( new ReentrantReadWriteLock( true ) );
    }

    public StoreCopyCheckPointMutex( ReadWriteLock lock )
    {
        this.lock = lock;
    }

    public Resource storeCopy( ThrowingAction<IOException> beforeFirstConcurrentStoreCopy ) throws IOException
    {
        Lock readLock = lock.readLock();
        boolean firstConcurrentRead = incrementCount() == 0;
        boolean success = false;
        try
        {
            if ( firstConcurrentRead )
            {
                try
                {
                    beforeFirstConcurrentStoreCopy.apply();
                }
                catch ( IOException e )
                {
                    storeCopyActionError = e;
                    throw e;
                }
                catch ( Throwable e )
                {
                    storeCopyActionError = e;
                    throw new IOException( e );
                }
                storeCopyActionCompleted = true;
            }
            else
            {
                // Wait for the "before" first store copy to complete
                waitForFirstStoreCopyActionToComplete();
            }
            success = true;
        }
        finally
        {
            if ( success )
            {
                readLock.lock();
            }
            else
            {
                decrementCount();
            }
        }

        return () ->
        {
            // Decrement concurrent store-copy count
            decrementCount();
            readLock.unlock();
        };
    }

    private void waitForFirstStoreCopyActionToComplete() throws IOException
    {
        while ( !storeCopyActionCompleted )
        {
            if ( storeCopyActionError != null )
            {
                throw new IOException( "Co-operative action before store-copy failed", storeCopyActionError );
            }
            parkAWhile();
        }
    }

    private synchronized void decrementCount()
    {
        storeCopyCount--;
        if ( storeCopyCount == 0 )
        {
            // If I'm the last one then also clear the other status fields so that a clean new session
            // can begin on the next store-copy request
            clear();
        }
    }

    private void clear()
    {
        storeCopyActionCompleted = false;
        storeCopyActionError = null;
    }

    private synchronized int incrementCount()
    {
        return storeCopyCount++;
    }

    private static void parkAWhile()
    {
        LockSupport.parkNanos( MILLISECONDS.toNanos( 100 ) );
    }

    public Resource tryCheckPoint()
    {
        Lock writeLock = lock.writeLock();
        return writeLock.tryLock() ? writeLock::unlock : null;
    }

    public Resource checkPoint()
    {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        return writeLock::unlock;
    }
}
