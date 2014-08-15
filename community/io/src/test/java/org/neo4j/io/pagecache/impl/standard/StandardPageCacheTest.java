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

import java.io.IOException;

import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCacheTest;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class StandardPageCacheTest extends PageCacheTest<StandardPageCache>
{
    @Override
    protected StandardPageCache createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor )
    {
        return new StandardPageCache( fs, maxPages, pageSize, monitor );
    }

    @Override
    protected void tearDownPageCache( StandardPageCache pageCache ) throws IOException
    {
        pageCache.close();
    }

    @Test
    public void shouldRemoveEvictedPages() throws Exception
    {
        // Given
        int pagesInCache = 64;
        int pagesKeptInUse = (int) (pagesInCache * ClockSweepPageTable.PAGE_UTILISATION_RATIO);

        StandardPageCache cache = getPageCache( fs, pagesInCache, filePageSize, PageCacheMonitor.NULL );
        StandardPagedFile pagedFile = (StandardPagedFile) cache.map( file, filePageSize );

        // When I pin and unpin a series of pages
        try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < 128; i++ )
            {
                cursor.next();
            }
        }

        // Then
        Thread.sleep( 50 );
        assertThat( pagedFile.numberOfCachedPages(),
                allOf( greaterThanOrEqualTo( pagesKeptInUse ), lessThanOrEqualTo( pagesInCache ) ) );
        cache.unmap( file );
    }
}
