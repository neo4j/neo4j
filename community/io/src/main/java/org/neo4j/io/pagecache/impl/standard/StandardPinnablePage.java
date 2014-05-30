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

public class StandardPinnablePage extends ByteBufferPage implements PinnablePage
{
    static final long UNBOUND_PAGE_ID = -1;
    static final byte MAX_USAGE_COUNT = 5;

    /** Used when the page is part of the free-list, points to next free page */
    public volatile StandardPinnablePage next;
    public volatile byte usageStamp;
    public volatile boolean loaded = false;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private PageIO io;
    private long pageId = UNBOUND_PAGE_ID;
    private boolean dirty;
    private int pageSize;

    public StandardPinnablePage( int pageSize )
    {
        super( null );
        this.pageSize = pageSize;
        dirty = false;
    }

    @Override
    public boolean pin( PageIO assertIO, long assertPageId, PageLock lockType )
    {
        lock( lockType );
        if( verifyPageBindings( assertIO, assertPageId ) )
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
        return false;
    }

    @Override
    public void unpin( PageLock lock )
    {
        unlock( lock );
    }

    void lock( PageLock lockType )
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

    void unlock( PageLock lockType )
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

    /**
     * Must be called while holding either a SHARED or an EXCLUSIVE lock, in order to prevent
     * racing with eviction.
     */
    private boolean verifyPageBindings( PageIO assertIO, long assertPageId )
    {
        return assertPageId == pageId && io == assertIO;
    }

    private void assertLocked()
    {
        // HotSpot will optimise this away completely, if assertions are not enabled
        // This is mostly here to fail our tests, if we do naughty stuff with locks
        assert lock.isWriteLockedByCurrentThread() || lock.getReadHoldCount() > 0: "Unsynchronised access";
    }

    /**
     * Must be call under lock
     */
    private ByteBuffer buffer()
    {
        assertLocked();
        if( buffer == null )
        {
            try
            {
                buffer = ByteBuffer.allocateDirect( pageSize );
            }
            catch( OutOfMemoryError e )
            {
                buffer = ByteBuffer.allocate( pageSize );
            }
        }
        return buffer;
    }

    /**
     * Must be call under lock
     */
    void reset( PageIO io, long pageId )
    {
        assertLocked();
        this.io = io;
        this.pageId = pageId;
    }

    /** Attempt to lock this page exclusively, used by page table during house keeping. */
    boolean tryExclusiveLock()
    {
        return lock.getReadLockCount() == 0
                && !lock.isWriteLocked()
                && lock.writeLock().tryLock();
    }

    void releaseExclusiveLock()
    {
        lock.writeLock().unlock();
    }

    /**
     * Must be call under lock
     */
    void flush() throws IOException
    {
        assertLocked();
        if ( dirty )
        {
            buffer().position(0);
            io.write( pageId, buffer );
            dirty = false;
        }
    }

    /**
     * Must be call under lock
     */
    void load() throws IOException
    {
        assertLocked();
        buffer().position(0);
        io.read( pageId, buffer );
        loaded = true;
    }

    /**
     * Must be call under lock
     */
    void evicted()
    {
        assertLocked();
        io.evicted( pageId );
    }

    /**
     * Must be call under lock
     */
    boolean isBackedBy( PageIO io )
    {
        assertLocked();
        return this.io != null && this.io.equals( io );
    }

    /**
     * Must be call under lock
     */
    PageIO io()
    {
        assertLocked();
        return io;
    }

    /**
     * Must be call under lock
     */
    long pageId()
    {
        assertLocked();
        return pageId;
    }

    /**
     * Should be call under lock
     */
    @Override
    public String toString()
    {
        return "StandardPinnablePage{" +
                "buffer=" + buffer +
                ", io=" + io +
                ", pageId=" + pageId +
                ", dirty=" + dirty +
                ", usageStamp=" + usageStamp +
                ", loaded=" + loaded +
                '}';
    }
}
