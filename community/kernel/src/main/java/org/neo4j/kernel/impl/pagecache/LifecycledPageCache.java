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
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class LifecycledPageCache extends LifecycleAdapter implements PageCache
{
    private final StandardPageCache pageCache;
    private final JobScheduler scheduler;
    private volatile JobScheduler.JobHandle pageEvictionJobHandle;

    public LifecycledPageCache( FileSystemAbstraction fs, JobScheduler scheduler, Config config )
    {
        this.scheduler = scheduler;
        this.pageCache = new StandardPageCache( fs, calculateMaxPages( config ), calculatePageSize( config ) );
    }

    private static int calculateMaxPages( Config config )
    {
        // TODO do something clever for calculating the number of pages
        return 1024;
    }

    private static int calculatePageSize( Config config )
    {
        // TODO do something clever for calculating the page size
        return 4096;
    }

    @Override
    public void start()
    {
        pageEvictionJobHandle = scheduler.schedule( JobScheduler.Group.pageCacheEviction, pageCache );
    }

    @Override
    public void stop()
    {
        JobScheduler.JobHandle handle = pageEvictionJobHandle;
        if ( handle != null )
        {
            handle.cancel( true );
        }
    }

    @Override
    public void close() throws IOException
    {
        pageCache.close();
    }

    @Override
    public PageCursor newCursor()
    {
        return pageCache.newCursor();
    }

    @Override
    public void unmap( File fileName ) throws IOException
    {
        pageCache.unmap( fileName );
    }

    @Override
    public PagedFile map( File file, int filePageSize ) throws IOException
    {
        return pageCache.map( file, filePageSize );
    }

    @Override
    public void flush()
    {
        pageCache.flush();
    }
}
