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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

public class PageCacheLongArray extends PageCacheNumberArray<LongArray> implements LongArray
{
    PageCacheLongArray( PagedFile pagedFile, long length, long defaultValue, long base ) throws IOException
    {
        super( pagedFile, Long.BYTES, length, defaultValue, base );
    }

    @Override
    public long get( long index )
    {
        long pageId = pageId( index );
        int offset = offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_READ_LOCK ) )
        {
            cursor.next();
            long result;
            do
            {
                result = cursor.getLong( offset );
            }
            while ( cursor.shouldRetry() );
            checkBounds( cursor );
            return result;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void set( long index, long value )
    {
        long pageId = pageId( index );
        int offset = offset( index );
        try ( PageCursor cursor = pagedFile.io( pageId, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            cursor.next();
            cursor.putLong( offset, value );
            checkBounds( cursor );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
