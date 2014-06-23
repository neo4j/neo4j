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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

/*
                                                                     ....
                                                               .;okKNWMUWN0ko,
       O'er Mithgarth Hugin and Munin both                   ;0WMUNINNMUNINNMUNOdc'.
       Each day set forth to fly;                          .OWMUNINNMUNI  00WMUNINNXko;.
       For Hugin I fear lest he come not home,            .KMUNINNMUNINNMWKKWMUNINNMUNIN0l.
       But for Munin my care is more.                    .KMUNINNMUNINNMUNINNWKkdlc:::::::,
                                                       .lXMUNINNMUNINNMUNINXo.
                                                   .,lONMUNINNMUNINNMUNINNk.
                                             .,cox0NMUNINNMUNINNMUNINNMUNI:
                                        .;dONMUNINNMUNINNMUNINNMUNINNMUNIN'
                                  .';okKWMUNINNMUNINNMUNINNMUNINNMUNINNMUx
                             .:dkKNWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNN'
                       .';lONMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNWl
                      .:okXWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNM0.
                  .,oONMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNo
            .';lx0NMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUN0.
         ;kKWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMWx.
       .,kWMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMXd'
  .;lkKNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMUNINNMNx;.
  .oNMUNINNMWNKOxoc;,,;:cdkKNWMUNINNMUNINNMUNINNMUNINNMUNINWKx;.
   lkOkkdl:,.                .':lkWMUNINNMUNINNMUNINN0kdoc;.
                                 c0WMUNINNMUNINNMUWx.
                                  .;ccllllxNMUNIXo'
                                          lWMUWkK;   .
                                          OMUNK.dNdc,....
                                          cWMUNlkWWWO:cl;.
                                           ;kWO,....',,,.
                                             cNd
                                              :Nk.
                                               cWK,
                                            .,ccxXWd.
                                                  dWNkxkOdc::;.
                                                   cNNo:ldo:.
                                                    'xo.   ..
*/
/**
 * <p>
 *     In Norse mythology, Huginn (from Old Norse "thought") and Muninn (Old Norse
 *     "memory" or "mind") are a pair of ravens that fly all over the world, Midgard,
 *     and bring information to the god Odin.
 * </p>
 * <p>
 *     This implementation of {@link org.neo4j.io.pagecache.PageCache} is optimised for
 *     configurations with large memory capacities and large stores, and uses Sequence-
 *     locks to make uncontended reads and writes fast.
 * </p>
 */
public class MuninnPageCache implements PageCache, Runnable
{
    private final FileSystemAbstraction fs;
    private final int pageSize;
    private final MuninnPage[] pages;

    // Linked list of mappings - guarded by synchronized(this)
    private volatile FileMapping mappedFiles;

    // Linked list of free pages - accessed through atomics (unsafe)
    private volatile MuninnPage freelist;

    // The thread that runs the eviction algorithm. We unpark this when we've run out of
    // free pages to grab.
    private volatile Thread evictorThread;

    public MuninnPageCache( FileSystemAbstraction fs, int maxPages, int pageSize )
    {
        this.fs = fs;
        this.pageSize = pageSize;
        this.pages = new MuninnPage[maxPages];

        for ( int i = 0; i < maxPages; i++ )
        {
            MuninnPage page = new MuninnPage( pageSize );
            pages[i] = page;
            page.nextFree = freelist;
            freelist = page;
        }
    }

    @Override
    public synchronized PagedFile map( File file, int pageSize ) throws IOException
    {
        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                MuninnPagedFile pagedFile = current.pagedFile;
                pagedFile.incrementReferences();
                return pagedFile;
            }
            current = current.next;
        }

        // there was no existing mapping
        MuninnPagedFile pagedFile = new MuninnPagedFile( pages, pageSize );
        current = new FileMapping( file, pagedFile );
        current.next = mappedFiles;
        mappedFiles = current;
        return pagedFile;
    }

    @Override
    public synchronized void unmap( File file ) throws IOException
    {
        FileMapping prev = null;
        FileMapping current = mappedFiles;

        // find an existing mapping
        while ( current != null )
        {
            if ( current.file.equals( file ) )
            {
                MuninnPagedFile pagedFile = current.pagedFile;
                if ( pagedFile.decrementReferences() )
                {
                    // this was the last reference; boot it from the list
                    if ( prev == null )
                    {
                        mappedFiles = current.next;
                    }
                    else
                    {
                        prev.next = current.next;
                    }
                    pagedFile.close();
                }
                break;
            }
            prev = current;
            current = current.next;
        }
    }

    @Override
    public void flush() throws IOException
    {

    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    @Override
    public int maxCachedPages()
    {
        return pages.length;
    }

    /**
     * Runs the eviction algorithm. Must be run in a dedicated thread.
     */
    @Override
    public void run()
    {
        // We scan through all the pages, one by one, and decrement their usage stamps.
        // If a usage reaches zero, we try-write-locking it, and if we get that lock,
        // we evict the page. If we don't, we move on to the next page.
        // Once we have enough free pages, we park our thread. Page-faulting will
        // unpark our thread as needed.
        evictorThread = Thread.currentThread();
    }
}
