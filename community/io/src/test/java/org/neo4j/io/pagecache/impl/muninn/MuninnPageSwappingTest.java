/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.function.Factory;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PageSwappingTest;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;

public class MuninnPageSwappingTest extends PageSwappingTest
{
    private static final PageSwapper NULL_SWAPPER = new PageSwapper()
    {
        @Override
        public int read( long filePageId, Page page ) throws IOException
        {
            return 0;
        }

        @Override
        public int write( long filePageId, Page page ) throws IOException
        {
            return 0;
        }

        @Override
        public void evicted( long pageId, Page page )
        {
        }

        @Override
        public String fileName()
        {
            return null;
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        public void force() throws IOException
        {
        }

        @Override
        public long getLastPageId() throws IOException
        {
            return 0;
        }
    };

    public MuninnPageSwappingTest( Factory<PageSwapperFactory> fixture )
    {
        super( fixture );
    }

    @Override
    protected Page createPage( int cachePageSize )
    {
        MemoryReleaser memoryReleaser = new MemoryReleaser( 1 );
        MuninnPage page = new MuninnPage( cachePageSize, 0, memoryReleaser );
        long stamp = page.writeLock();
        try
        {
            // We have to do this to initialise the native memory pointer
            page.fault( NULL_SWAPPER, 0, PageCacheMonitor.NULL_PAGE_FAULT_EVENT );
        }
        catch ( IOException e )
        {
            throw new AssertionError( e );
        }
        finally
        {
            page.unlockWrite( stamp );
        }
        return page;
    }

    @Override
    public long writeLock( Page page )
    {
        return ((MuninnPage) page).writeLock();
    }

    @Override
    public void unlockWrite( Page page, long stamp )
    {
        ((MuninnPage) page).unlockWrite( stamp );
    }
}
