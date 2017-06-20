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
package org.neo4j.impl.store.cursors;

import static java.lang.String.format;

abstract class MemoryAccess
{
    long virtualAddress;
    PageHandle page;
    long pageId;
    long base;
    int offset;
    long lockToken;

    protected abstract int dataBound();

    final void closeAccess()
    {
        if ( page != null )
        {
            try
            {
                page.releasePage( pageId, base, offset, lockToken );
            }
            finally
            {
                page = null;
            }
        }
    }

    public final boolean hasPageReference()
    {
        return page != null;
    }

    final void access( long virtualAddress, PageHandle page, long pageId, long base, int offset )
    {
        // TODO: this method is too large to inline...
        if ( this.page != null )
        {
            if ( this.page != page || this.pageId != pageId )
            {
                closeAccess();
            }
            else
            {
                lockRelease(); // TODO: this is wrong - the move method assumes that the lock has NOT been released!
            }
        }
        this.virtualAddress = virtualAddress;
        this.page = page;
        this.pageId = pageId;
        this.base = base;
        this.offset = offset;
    }

    final void lockShared()
    {
        lockToken = page.sharedLock( pageId, base, offset );
    }

    final void lockExclusive()
    {
        lockToken = page.exclusiveLock( pageId, base, offset );
    }

    final void lockRelease()
    {
        page.releaseLock( pageId, base, offset, lockToken );
        lockToken = 0;
    }

    final long address( int offset, int size )
    {
        if ( page == null )
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
