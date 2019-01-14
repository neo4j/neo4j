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

import static java.lang.Math.toIntExact;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;

/**
 * Abstraction over page cache backed number arrays.
 *
 * @see PageCachedNumberArrayFactory
 */
public abstract class PageCacheNumberArray<N extends NumberArray<N>> implements NumberArray<N>
{
    protected final PagedFile pagedFile;
    protected final int entriesPerPage;
    protected final int entrySize;
    private final long length;
    private final long defaultValue;
    private final long base;
    private boolean closed;

    PageCacheNumberArray( PagedFile pagedFile, int entrySize, long length, long base ) throws IOException
    {
        this( pagedFile, entrySize, length, 0, base );
    }

    PageCacheNumberArray( PagedFile pagedFile, int entrySize, long length, long defaultValue, long base )
            throws IOException
    {
        this.pagedFile = pagedFile;
        this.entrySize = entrySize;
        this.entriesPerPage = pagedFile.pageSize() / entrySize;
        this.length = length;
        this.defaultValue = defaultValue;
        this.base = base;

        try ( PageCursor cursorToSetLength = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
        {
            setLength( cursorToSetLength, length );
        }

        if ( defaultValue != 0 )
        {
            setDefaultValue( defaultValue );
        }
    }

    private void setLength( PageCursor cursor, long length ) throws IOException
    {
        if ( !cursor.next( (length - 1) / entriesPerPage ) )
        {
            throw new IllegalStateException(
                    String.format( "Unable to extend the backing file %s to desired size %d.", pagedFile, length ) );
        }
    }

    protected long pageId( long index )
    {
        return rebase( index ) / entriesPerPage;
    }

    protected int offset( long index )
    {
        return toIntExact( rebase( index ) % entriesPerPage * entrySize );
    }

    private long rebase( long index )
    {
        return index - base;
    }

    protected void setDefaultValue( long defaultValue ) throws IOException
    {
        try ( PageCursor writeCursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
        {
            writeCursor.next();
            int pageSize = pagedFile.pageSize();
            fillPageWithDefaultValue( writeCursor, defaultValue, pageSize );
            if ( pageId( length - 1 ) > 0 )
            {
                try ( PageCursor cursor = pagedFile.io( 1, PF_NO_GROW | PF_SHARED_WRITE_LOCK ) )
                {
                    while ( cursor.next() )
                    {
                        writeCursor.copyTo( 0, cursor, 0, pageSize );
                        checkBounds( writeCursor );
                    }
                }
            }
        }
    }

    protected void fillPageWithDefaultValue( PageCursor writeCursor, long defaultValue, int pageSize )
    {
        int longsInPage = pageSize / Long.BYTES;
        for ( int i = 0; i < longsInPage; i++ )
        {
            writeCursor.putLong( defaultValue );
        }
        checkBounds( writeCursor );
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public void clear()
    {
        try
        {
            setDefaultValue( defaultValue );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            return;
        }
        try
        {
            pagedFile.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            closed = true;
        }
    }

    @Override
    public N at( long index )
    {
        return (N) this;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( length() * entrySize );
    }

    protected void checkBounds( PageCursor cursor )
    {
        if ( cursor.checkAndClearBoundsFlag() )
        {
            throw new IllegalStateException(
                    String.format( "Cursor %s access out of bounds, page id %d, offset %d", cursor.toString(),
                            cursor.getCurrentPageId(), cursor.getOffset() ) );
        }
    }
}
