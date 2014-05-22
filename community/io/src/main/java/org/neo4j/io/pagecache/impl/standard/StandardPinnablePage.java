/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.ByteBufferPage;

public class StandardPinnablePage extends ByteBufferPage implements PageTable.PinnablePage
{
    static final byte MAX_USAGE_COUNT = 5;

    /** Used when the page is part of the free-list, points to next free page */
    public volatile StandardPinnablePage next;
    public volatile byte usageStamp;
    public volatile boolean loaded = false;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private PageTable.PageIO io;
    private long pageId = -1;
    private boolean dirty;

    public StandardPinnablePage( ByteBuffer buffer )
    {
        super(buffer);
        dirty = false;
    }

    @Override
    public boolean pin( PageTable.PageIO assertIO, long assertPageId, PageLock lockType )
    {
        if(assertionsHold( assertIO, assertPageId ))
        {
            lock(lockType);
            if(assertionsHold( assertIO, assertPageId ))
            {
                byte stamp = usageStamp;
                if ( stamp < MAX_USAGE_COUNT )
                {
                    // Racy, but we don't care
                    usageStamp = (byte) (stamp + 1);
                }
                return true;
            }
            else
            {
                unpin( lockType );
            }
        }
        return false;
    }

    @Override
    public void unpin( PageLock lock )
    {
        unlock( lock );
    }

    public void lock( PageLock lockType )
    {
        if(lockType == PageLock.SHARED)
        {
            lock.readLock().lock();
        }
        else if( lockType == PageLock.EXCLUSIVE )
        {
            lock.writeLock().lock();
            dirty = true;
        }
        else
        {
            throw new IllegalArgumentException( "Unknown lock type: " + lockType );
        }
    }

    public void unlock( PageLock lockType )
    {
        if( lockType == PageLock.SHARED)
        {
            this.lock.readLock().unlock();
        }
        else if( lockType == PageLock.EXCLUSIVE )
        {
            this.lock.writeLock().unlock();
        }
        else
        {
            throw new IllegalArgumentException( "Unknown lock type: " + lockType );
        }
    }

    private boolean assertionsHold( PageTable.PageIO assertIO, long assertPageId )
    {
        return assertPageId == pageId && io != null && io.equals( assertIO );
    }

    public ByteBuffer buffer()
    {
        return buffer;
    }

    public void reset( PageTable.PageIO io, long pageId )
    {
        this.io = io;
        this.pageId = pageId;
    }

    /** Attempt to lock this page exclusively, used by page table during house keeping. */
    public boolean tryExclusiveLock()
    {
        return lock.getReadLockCount() == 0
                && !lock.isWriteLocked()
                && lock.writeLock().tryLock();
    }

    public void releaseExclusiveLock()
    {
        lock.writeLock().unlock();
    }

    public void flush() throws IOException
    {
        if ( dirty )
        {
            buffer.position(0);
            io.write( pageId, buffer );
            dirty = false;
        }
    }

    public void load() throws IOException
    {
        buffer.position(0);
        io.read( pageId, buffer );
        loaded = true;
    }

    public void evicted()
    {
        io.evicted( pageId );
    }
}
