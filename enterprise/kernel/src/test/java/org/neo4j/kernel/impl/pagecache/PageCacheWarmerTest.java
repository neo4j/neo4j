/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.pagecache;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
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
        scheduler = life.add( new CentralJobScheduler() );
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
    public void doNotReheatAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.reheat() );
        }
    }

    @Test
    public void doNoProfileAfterStop() throws IOException
    {
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile ignore = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.start();
            warmer.stop();
            assertSame( OptionalLong.empty(), warmer.profile() );
        }
    }

    @Test
    public void profileAndReheatAfterRestart() throws IOException
    {
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.start();
            warmer.stop();
            warmer.start();
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( writer.next( 1 ) );
                assertTrue( writer.next( 3 ) );
            }
            warmer.profile();
            assertNotSame( OptionalLong.empty(), warmer.reheat() );
        }
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
            warmer.start();
            warmer.profile();
        }

        clearTracerCounts();
        long initialFaults = cacheTracer.faults();
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.start();
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 2L ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                assertTrue( reader.next( 1 ) );
                assertTrue( reader.next( 3 ) );
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + 2L ) );
        }
    }

    @Test
    public void reheatingMustWorkOnLargeNumberOfPages() throws Exception
    {
        int maxPagesInMemory = 1_000;
        int[] pageIds = randomSortedPageIds( maxPagesInMemory );

        String pageCacheMemory = String.valueOf( maxPagesInMemory * ByteUnit.kibiBytes( 9 ) );
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg.withMemory( pageCacheMemory ) );
              PagedFile pf = pageCache.map( file, pageCache.pageSize(), StandardOpenOption.CREATE ) )
        {
            try ( PageCursor writer = pf.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( writer.next( pageId ) );
                }
            }
            pf.flushAndForce();
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.profile();
        }

        long initialFaults = cacheTracer.faults();
        clearTracerCounts();
        try ( PageCache pageCache = pageCacheRule.getPageCache( fs, cfg );
              PagedFile pf = pageCache.map( file, pageCache.pageSize() ) )
        {
            PageCacheWarmer warmer = new PageCacheWarmer( fs, pageCache, scheduler );
            warmer.start();
            warmer.reheat();

            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + pageIds.length ) );

            try ( PageCursor reader = pf.io( 0, PagedFile.PF_SHARED_READ_LOCK ) )
            {
                for ( int pageId : pageIds )
                {
                    assertTrue( reader.next( pageId ) );
                }
            }

            // No additional faults must have been reported.
            pageCache.reportEvents();
            assertThat( cacheTracer.faults(), is( initialFaults + pageIds.length ) );
        }
    }

    @SuppressWarnings( "unused" )
    @Test
    public void profileMustNotDeleteFilesCurrentlyExposedViaFileListing() throws Exception
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
            warmer.start();
            warmer.profile();
            warmer.profile();
            warmer.profile();

            List<StoreFileMetadata> fileListing = new ArrayList<>();
            try ( Resource firstListing = warmer.addFilesTo( fileListing ) )
            {
                warmer.profile();
                warmer.profile();

                // The files in the file listing cannot be deleted while the listing is in use.
                assertThat( fileListing.size(), greaterThan( 0 ) );
                assertFilesExists( fileListing );
                warmer.profile();
                try ( Resource secondListing = warmer.addFilesTo( new ArrayList<>() ) )
                {
                    warmer.profile();
                    // This must hold even when there are file listings overlapping in time.
                    assertFilesExists( fileListing );
                }
                warmer.profile();
                // And continue to hold after other overlapping listing finishes.
                assertFilesExists( fileListing );
            }
            // Once we are done with the file listing, profile should remove those files.
            warmer.profile();
            warmer.stop();
            assertFilesNotExists( fileListing );
        }
    }

    @Test
    public void profilesMustSortByPagedFileAndProfileSequenceId()
    {
        File fileAA = new File( "aa" );
        File fileAB = new File( "ab" );
        File fileBA = new File( "ba" );
        Profile aa;
        Profile ab;
        Profile ba;
        List<Profile> sortedProfiles = Arrays.asList(
                aa = Profile.first( fileAA ),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(),
                aa = aa.next(), aa = aa.next(), aa = aa.next(), aa = aa.next(), aa.next(),
                ab = Profile.first( fileAB ),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(),
                ab = ab.next(), ab = ab.next(), ab = ab.next(), ab = ab.next(), ab.next(),
                ba = Profile.first( fileBA ),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(),
                ba = ba.next(), ba = ba.next(), ba = ba.next(), ba = ba.next(), ba.next()
        );
        List<Profile> resortedProfiles = new ArrayList<>( sortedProfiles );
        Collections.shuffle( resortedProfiles );
        Collections.sort( resortedProfiles );
        assertThat( resortedProfiles, is( sortedProfiles ) );
    }

    private void assertFilesExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertTrue( fs.fileExists( fileMetadata.file() ) );
        }
    }

    private void assertFilesNotExists( List<StoreFileMetadata> fileListing )
    {
        for ( StoreFileMetadata fileMetadata : fileListing )
        {
            assertFalse( fs.fileExists( fileMetadata.file() ) );
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
