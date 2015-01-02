/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.concurrent.atomic.AtomicInteger;

class BrickElement
{
    private final int index;
    private int hitCount;
    private int hitCountSnapshot;
    private volatile LockableWindow window;
    final AtomicInteger lockCount = new AtomicInteger();

    BrickElement( int index )
    {
        this.index = index;
    }

    void setWindow( LockableWindow window )
    {
        this.window = window;
    }

    LockableWindow getWindow()
    {
        return window;
    }

    int index()
    {
        return index;
    }

    void setHit()
    {
        hitCount += 10;
        if ( hitCount < 0 )
        {
            hitCount -= 10;
        }
    }

    int getHit()
    {
        return hitCount;
    }

    void refresh()
    {
        if ( window == null )
        {
            hitCount /= 1.25;
        }
        else
        {
            hitCount /= 1.15;
        }
    }

    void snapshotHitCount()
    {
        hitCountSnapshot = hitCount;
    }

    int getHitCountSnapshot()
    {
        return hitCountSnapshot;
    }

    LockableWindow getAndMarkWindow()
    {
        try
        {
            // Happy path if we have a valid window we can mark it and return it
            LockableWindow candidate = window;
            if ( candidate != null && candidate.markAsInUse() )
            {
                return candidate;
            }

            /* We may have to allocate a row over this position, so we first need to increase the row count over
             * this brick to make sure that if a refreshBricks() runs at the same time it won't map a window
             * under this row. Locking has to happen before we get the window, otherwise we open up for a race
             * between checking for the window and a refreshBricks(). */
            lock();
            candidate = window;

            // Option 1 we see a null window or a closed window then we return null and increment lock count on brick
            // Option 2 we see a valid window and mark it as in use and return it leaving brick lock count unchanged
            if ( candidate != null && candidate.markAsInUse() )
            {
                // This means the position is in a window and not in a row, so unlock.
                unLock();
                return candidate;
            }
            return null;
        }
        finally
        {
            setHit();
        }
    }

    synchronized void lock()
    {
        lockCount.incrementAndGet();
    }

    /**
     * Not synchronized on purpose.
     * See {@link PersistenceWindowPool#allocateNewWindow(BrickElement)} for details.
     */
    void unLock()
    {
        int lockCountAfterDecrement = lockCount.decrementAndGet();
        assert lockCountAfterDecrement >= 0 : "Should not be able to have negative lock count " + lockCountAfterDecrement;
    }

    @Override
    public String toString()
    {
        return "" + hitCount + (window == null ? "x" : "o");
    }
}
