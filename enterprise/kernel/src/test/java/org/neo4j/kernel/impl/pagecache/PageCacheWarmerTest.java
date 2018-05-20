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
import java.nio.file.StandardOpenOption;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.scheduler.JobScheduler;
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

    private JobScheduler scheduler;
    private DefaultPageCacheTracer cacheTracer;
    private PageCacheRule.PageCacheConfig cfg;
    private File file;

    @Before
    public void setUp() throws Throwable
    {
        scheduler = new Neo4jJobScheduler();
        scheduler.init();
        scheduler.start();
        DefaultPageCursorTracerSupplier.INSTANCE.get().init( PageCacheTracer.NULL );
        DefaultPageCursorTracerSupplier.INSTANCE.get().reportEvents();
        cacheTracer = new DefaultPageCacheTracer();
        DefaultPageCursorTracerSupplier.INSTANCE.get().init( cacheTracer );
        cfg = PageCacheRule.config().withTracer( cacheTracer );
        file = dir.file( "a" );
        fs.create( file );
    }

    @After
    public void tearDown() throws Throwable
    {
        scheduler.stop();
        scheduler.shutdown();
    }

    @Test
    public void mustReheatPageCache() throws Exception
    {
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( writer.next() );
                assertTrue( writer.next() );
                assertTrue( writer.next() );
                assertTrue( writer.next() );
            }
            pf.flushAndForce();
            pageCache.reportEvents();
        }

        assertThat( cacheTracer.faults(), is( 4L ) );

        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( pageCache, scheduler );
            warmer.start();
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( 8L ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                assertTrue( reader.next( 1 ) );
                assertTrue( reader.next( 3 ) );
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( 8L ) );
        }
    }
}
