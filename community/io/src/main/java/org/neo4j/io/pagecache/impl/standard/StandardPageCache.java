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
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Your average run-of-the-mill page cache.
 */
public class StandardPageCache implements PageCache, Runnable
{
    private final FileSystemAbstraction fs;
    private final PageCacheMonitor monitor;
    private final Map<File, StandardPagedFile> pagedFiles = new HashMap<>();
    private final ClockSweepPageTable table;

    private boolean closed; // Guarded by synchronised(this)

    public StandardPageCache( FileSystemAbstraction fs, int maxPages, int pageSize )
    {
        this( fs, maxPages, pageSize, PageCacheMonitor.NULL );
    }

    public StandardPageCache( FileSystemAbstraction fs, int maxPages, int pageSize, PageCacheMonitor monitor )
    {
        this.fs = fs;
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
            StoreChannel channel = fs.open( file, "rw" );
            pagedFile = new StandardPagedFile( table, file, channel, filePageSize, monitor );
            pagedFiles.put( file, pagedFile );
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
        for ( StandardPagedFile file : pagedFiles.values() )
        {
            file.force();
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        // TODO what do we do if people still have files mapped and are using them?
        // We can't just close their files out from under them. It would be rude.
        // We also cannot just wait for them to unmap their files, because this method
        // synchronises on the same lock that unmap does.
        closed = true;
        table.flush();
        for ( StandardPagedFile file : pagedFiles.values() )
        {
            file.close();
        }
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
