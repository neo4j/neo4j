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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;

/**
 * Your average run-of-the-mill page cache.
 */
public class StandardPageCache implements RunnablePageCache
{
    private final PageSwapperFactory swapperFactory;
    private final PageCacheMonitor monitor;
    private final Map<File, StandardPagedFile> pagedFiles = new HashMap<>();
    private final ClockSweepPageTable table;

    private boolean closed; // Guarded by synchronised(this)

    StandardPageCache( FileSystemAbstraction fs, int maxPages, int pageSize, PageCacheMonitor monitor )
    {
        this( new SingleFilePageSwapperFactory( fs ), maxPages, pageSize, monitor );
    }

    public StandardPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor )
    {
        this.swapperFactory = swapperFactory;
        this.monitor = monitor;
        this.table = new ClockSweepPageTable( maxPages, pageSize, monitor );
    }

    @Override
    public synchronized PagedFile map( File file, int filePageSize ) throws IOException
    {
        assertNotClosed();
        if ( filePageSize > table.pageSize() )
        {
            throw new IllegalArgumentException( "Cannot map files with a filePageSize (" +
                    filePageSize + ") that is greater than the cachePageSize (" +
                    table.pageSize() + ")" );
        }

        StandardPagedFile pagedFile = pagedFiles.get( file );
        if ( pagedFile == null || !pagedFile.claimReference() )
        {
            pagedFile = new StandardPagedFile( table, file, swapperFactory, filePageSize, monitor );
            pagedFiles.put( file, pagedFile );
        }

        if ( pagedFile.pageSize() != filePageSize )
        {
            pagedFile.releaseReference(); // Oops, can't use it after all.
            String msg = "Cannot map file " + file + " with " +
                    "filePageSize " + filePageSize + " bytes, " +
                    "because it has already been mapped with a " +
                    "filePageSize of " + pagedFile.pageSize() +
                    " bytes.";
            throw new IllegalArgumentException( msg );
        }

        return pagedFile;
    }

    @Override
    public synchronized void unmap( File fileName ) throws IOException
    {
        StandardPagedFile file = pagedFiles.get( fileName );
        if(file != null && file.releaseReference())
        {
            file.flush();
            file.close();
            pagedFiles.remove( fileName );
        }
    }

    @Override
    public void flush() throws IOException
    {
        assertNotClosed();
        table.flush();
        synchronized ( this )
        {
            for ( StandardPagedFile file : pagedFiles.values() )
            {
                file.force();
            }
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( closed )
        {
            return;
        }

        if ( !pagedFiles.isEmpty() )
        {
            throw new IllegalStateException(
                    "Cannot close the PageCache while files are still mapped." );
        }

        closed = true;
    }

    private void assertNotClosed()
    {
        if ( closed )
        {
            throw new IllegalStateException( "The PageCache has been shut down" );
        }
    }

    /** Run the eviction algorithm until interrupted. */
    @Override
    public void run()
    {
        table.run();
    }

    @Override
    public int pageSize()
    {
        return table.pageSize();
    }

    @Override
    public int maxCachedPages()
    {
        return table.maxCachedPages();
    }
}
