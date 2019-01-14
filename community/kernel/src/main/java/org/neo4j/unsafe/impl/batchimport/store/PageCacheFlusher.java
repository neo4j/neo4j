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
package org.neo4j.unsafe.impl.batchimport.store;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.helpers.Exceptions.throwIfUnchecked;

/**
 * A dedicated thread which constantly call {@link PageCache#flushAndForce()} until a call to {@link #halt()} is made.
 * Must be started manually by calling {@link #start()}.
 */
class PageCacheFlusher extends Thread
{
    private final PageCache pageCache;
    private final BinaryLatch halt = new BinaryLatch();
    private volatile boolean halted;
    private volatile Throwable error;

    PageCacheFlusher( PageCache pageCache )
    {
        super( "PageCacheFlusher" );
        this.pageCache = pageCache;
    }

    @Override
    public void run()
    {
        try
        {
            while ( !halted )
            {
                try
                {
                    pageCache.flushAndForce();
                }
                catch ( Throwable e )
                {
                    error = e;
                    break;
                }
            }
        }
        finally
        {
            halt.release();
        }
    }

    /**
     * Halts this flusher, making it stop flushing. The current call to {@link PageCache#flushAndForce()}
     * will complete before exiting this method call. If there was an error in the thread doing the flushes
     * that exception will be thrown from this method as a {@link RuntimeException}.
     */
    void halt()
    {
        halted = true;
        halt.await();
        if ( error != null )
        {
            throwIfUnchecked( error );
            throw new RuntimeException(  error );
        }
    }
}
