/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/**
 * A combined linked-list of threads waiting for pages to be evicted and freed,
 * and exchanger of pages.
 *
 * If there are no free pages to page fault into, we'll have to wake up the
 * eviction thread and wait for it to free some pages.
 * Calling unpark a lot can be expensive and slow other threads down.
 * Especially on Windows. That's why we will eventually fall back on blocking
 * if we find that we spin too much. This class implements that blocking
 * support, and is used in MuninnPageCache.unparkEvictor on behalf of the page
 * faulting threads.
 */
final class FreePageWaiter
{
    // A special poison-pill value that is used to tell FreePageWaiters that they should
    // stop waiting and that there is no free page for them, because the page cache is
    // shutting down.
    private static final MuninnPage interruptSignal = new MuninnPage( 0, null );

    // Like the interruptSignal above, this is used to tell the FreePageWaiters that they
    // should stop waiting, but this time the reason is that the eviction thread has
    // encountered an exception, which must be bubbled out.
    private static final MuninnPage exceptionSignal = new MuninnPage( 0, null );

    FreePageWaiter next;

    private final Thread waiter;
    private IOException exception;
    private volatile MuninnPage page;

    public FreePageWaiter()
    {
        waiter = Thread.currentThread();
    }

    /**
     * Park and wait for a page to be transferred into the current threads possession.
     *
     * Returns 'null' if the page cache has been interrupted and is shutting down.
     *
     * Throws an IOException if the page cache has encountered an exception while trying
     * to evict pages.
     */
    public MuninnPage park( MuninnPageCache muninnPageCache ) throws IOException
    {
        MuninnPage page;
        do
        {
            LockSupport.park( muninnPageCache );
            page = this.page;
        }
        while ( page == null );

        if ( page == exceptionSignal )
        {
            throw new IOException( "Exception in the page eviction thread", exception );
        }

        return page == interruptSignal? null : page;
    }

    /**
     * Unpark the waiting thread and transfer the given Page to them.
     *
     * The given page cannot be null, since the waiting thread cannot
     * distinguish a null-transfer from a spurious wake-up.
     */
    public void unpark( MuninnPage page )
    {
        this.page = page;
        LockSupport.unpark( waiter );
    }

    /**
     * Unpark the waiting thread and let them know that the eviction thread has
     * been interrupted and that the page cache is shutting down.
     */
    public void unparkInterrupt()
    {
        this.page = interruptSignal;
        LockSupport.unpark( waiter );
    }

    /**
     * Unpark the waiting thread and let it receive the given exception as the reason
     * for why its request for a free page could not be fulfilled.
     */
    public void unparkException( IOException exception )
    {
        this.exception = exception;
        this.page = exceptionSignal;
        LockSupport.unpark( waiter );
    }

    @Override
    public String toString()
    {
        return shortString() + " -> " + (next == null ? "null" : next.shortString());
    }

    private String shortString()
    {
        return "FreePageWaiter@" + Integer.toHexString( hashCode() ) + "[t:" + waiter.getId() + "]";
    }
}
