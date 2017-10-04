/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.cursors;

import static java.lang.String.format;

abstract class MemoryAccess
{
    long virtualAddress;
    PageManager pageMan;
    long pageId;
    long base;
    int offset;
    long lockToken;

    protected abstract int dataBound();

    final void closeAccess()
    {
        if ( pageMan != null )
        {
            try
            {
                pageMan.releasePage( pageId, base, offset, lockToken );
            }
            finally
            {
                pageMan = null;
            }
        }
    }

    public final boolean hasPageReference()
    {
        return pageMan != null;
    }

    final void initializeMemoryAccess( long virtualAddress, PageManager pageMan, long pageId, long base, int offset )
    {
        if ( this.pageMan != null )
        {
            if ( this.pageMan != pageMan || this.pageId != pageId )
            {
                closeAccess();
            }
            else
            {
                lockRelease();
            }
        }
        this.virtualAddress = virtualAddress;
        this.pageMan = pageMan;
        this.pageId = pageId;
        this.base = base;
        this.offset = offset;
    }

    final void moveToOtherPage( long virtualAddress, long pageId, long base, int offset )
    {
        assert this.pageMan != null : "Memory access must be initialized before moving";
        this.virtualAddress = virtualAddress;
        this.pageId = pageId;
        this.base = base;
        this.offset = offset;
    }

    final void moveWithinPage( long virtualAddress, int offset )
    {
        assert this.pageMan != null : "Memory access must be initialized before moving";
        this.virtualAddress = virtualAddress;
        this.offset = offset;
    }

    final void lockShared()
    {
        lockToken = pageMan.sharedLock( pageId, base, offset );
    }

    final void lockExclusive()
    {
        lockToken = pageMan.exclusiveLock( pageId, base, offset );
    }

    final void lockRelease()
    {
        pageMan.releaseLock( pageId, base, offset, lockToken );
        lockToken = 0;
    }

    final long address( int offset, int size )
    {
        if ( pageMan == null )
        {
            throw new IllegalStateException( "Cursor has not been initialized." );
        }
        assert withinBounds( offset, size );
        return this.base + this.offset + offset;
    }

    private boolean withinBounds( int offset, int size )
    {
        int bound = dataBound();
        if ( offset + size > bound )
        {
            throw new IndexOutOfBoundsException( format(
                    "This cursor is bounded to %d bytes, tried to access %d bytes at offset %d.",
                    bound, size, offset ) );
        }
        return true;
    }
}
