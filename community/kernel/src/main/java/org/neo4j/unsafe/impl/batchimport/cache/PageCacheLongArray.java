/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.IOException;

import org.neo4j.io.pagecache.PagedFile;

import static java.lang.Math.toIntExact;

public class PageCacheLongArray extends PageCacheNumberArray<LongArray> implements LongArray
{
    private static final int ENTRY_SIZE = Long.BYTES;
    private static final int ENTRIES_PER_PAGE = PAGE_SIZE / ENTRY_SIZE;

    public PageCacheLongArray( PagedFile pagedFile ) throws IOException
    {
        super( pagedFile, ENTRIES_PER_PAGE, ENTRY_SIZE );
    }

    @Override
    public long get( long index )
    {
        long pageId = index / ENTRIES_PER_PAGE;
        int offset = toIntExact( index % ENTRIES_PER_PAGE ) * ENTRY_SIZE;
        if ( writeCursor.getCurrentPageId() == pageId )
        {
            // We have to read from the write cursor, since the write cursor is on it
            return writeCursor.getLong( offset );
        }

        // Go ahead and read from the read cursor
        try
        {
            goTo( readCursor, pageId );
            long result;
            do
            {
                result = readCursor.getLong( offset );
            }
            while ( readCursor.shouldRetry() );
            checkBounds( readCursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void set( long index, long value )
    {
        long pageId = index / ENTRIES_PER_PAGE;
        int offset = toIntExact( index % ENTRIES_PER_PAGE ) * ENTRY_SIZE;
        try
        {
            goTo( writeCursor, pageId );
            writeCursor.putLong( offset, value );
            checkBounds( writeCursor );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
