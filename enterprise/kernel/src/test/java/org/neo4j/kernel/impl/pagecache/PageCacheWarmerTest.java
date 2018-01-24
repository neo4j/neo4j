/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PageCacheWarmerTest
{
    private FileSystemRule fs = new EphemeralFileSystemRule();
    private TestDirectory dir = TestDirectory.testDirectory( fs );
    private PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public RuleChain rules = RuleChain.outerRule( fs ).around( dir ).around( pageCacheRule );

    private LifeSupport life;
    private JobScheduler scheduler;
    private DefaultPageCacheTracer cacheTracer;
    private DefaultPageCursorTracerSupplier cursorTracer;
    private PageCacheRule.PageCacheConfig cfg;
    private File file;

    @Before
    public void setUp() throws IOException
    {
        life = new LifeSupport();
        scheduler = life.add( new Neo4jJobScheduler() );
        life.start();
        cacheTracer = new DefaultPageCacheTracer();
        cursorTracer = DefaultPageCursorTracerSupplier.INSTANCE;
        clearTracerCounts();
        cfg = PageCacheRule.config().withTracer( cacheTracer ).withCursorTracerSupplier( cursorTracer );
        file = dir.file( "a" );
        fs.create( file );
    }

    @After
    public void tearDown()
    {
        life.shutdown();
    }

    private void clearTracerCounts()
    {
        cursorTracer.get().init( PageCacheTracer.NULL );
        cursorTracer.get().reportEvents();
        cursorTracer.get().init( cacheTracer );
    }

    @Test
    public void mustDoNothingWhenReheatingUnprofiledPageCache() throws Exception
    {

        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.reheat();
        }
        cursorTracer.get().reportEvents();
        assertThat( cacheTracer.faults(), is( 0L ) );
    }

    @Test
    public void mustReheatProfiledPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.profile();
        }

        clearTracerCounts();
        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 4L ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                assertTrue( reader.next( 1 ) );
                assertTrue( reader.next( 3 ) );
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 4L ) );
        }
    }

    private int[] randomSortedPageIds( int maxPagesInMemory )
    {
        PrimitiveIntSet setIds = Primitive.intSet();
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        for ( int i = 0; i < maxPagesInMemory; i++ )
        {
            setIds.add( rng.nextInt( maxPagesInMemory * 7 ) );
        }
        int[] pageIds = new int[setIds.size()];
        PrimitiveIntIterator itr = setIds.iterator();
        int i = 0;
        while ( itr.hasNext() )
        {
            pageIds[i] = itr.next();
            i++;
        }
        Arrays.sort( pageIds );
        return pageIds;
    }
}
