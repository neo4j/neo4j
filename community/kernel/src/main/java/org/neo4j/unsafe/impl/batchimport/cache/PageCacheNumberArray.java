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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.io.ByteUnit.kibiBytes;

public abstract class PageCacheNumberArray<N extends NumberArray<N>> implements NumberArray<N>
{
    static final int PAGE_SIZE = (int) kibiBytes( 8 );

    protected final PagedFile pagedFile;
    protected final PageCursor readCursor;
    protected final PageCursor writeCursor;
    private final int entriesPerPage;
    private final int entrySize;

    public PageCacheNumberArray( PagedFile pagedFile, int entriesPerPage, int entrySize ) throws IOException
    {
        this.pagedFile = pagedFile;
        this.entriesPerPage = entriesPerPage;
        this.entrySize = entrySize;
        this.readCursor = pagedFile.io( 0, PagedFile.PF_SHARED_READ_LOCK );
        this.writeCursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK );
        goTo( writeCursor, 0 );
    }

    @Override
    public long length()
    {
        try
        {
            return pagedFile.getLastPageId() * entriesPerPage;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void clear()
    {
    }

    @Override
    public void close()
    {
        readCursor.close();
        writeCursor.close();
        try
        {
            pagedFile.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
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
            throw new IllegalStateException();
        }
    }

    protected void goTo( PageCursor cursor, long pageId ) throws IOException
    {
        if ( !cursor.next( pageId ) )
        {
            throw new IllegalStateException();
        }
    }
}
