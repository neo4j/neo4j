/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCacheTest;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.ConfigurablePageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.DelegatingPageCacheTracer;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCacheTracer;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCursorTracer;
import org.neo4j.io.pagecache.tracing.recording.RecordingPageCursorTracer.Fault;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_READ_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.tracing.recording.RecordingPageCacheTracer.Evict;

public class MuninnPageCacheTest extends PageCacheTest<MuninnPageCache>
{
    private final long x = 0xCAFEBABEDEADBEEFL;
    private final long y = 0xDECAFC0FFEEDECAFL;
    private MuninnPageCacheFixture fixture;

    @Override
    protected Fixture<MuninnPageCache> createFixture()
    {
        return fixture = new MuninnPageCacheFixture();
    }

    private PageCacheTracer blockCacheFlush( PageCacheTracer delegate )
    {
        fixture.backgroundFlushLatch = new CountDownLatch( 1 );
        return new DelegatingPageCacheTracer( delegate )
        {
            @Override
            public MajorFlushEvent beginCacheFlush()
            {
                try
                {
                    fixture.backgroundFlushLatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
                return super.beginCacheFlush();
            }
        };
    }

    @Test
    public void ableToEvictAllPageInAPageCache() throws IOException
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier<RecordingPageCursorTracer> cursorTracerSupplier =
                new ConfigurablePageCursorTracerSupplier<>( cursorTracer );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, blockCacheFlush( tracer ), cursorTracerSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
            }
            try ( PageCursor cursor = pagedFile.io( 1, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
            }
            evictAllPages( pageCache );
        }
    }

    @Test
    public void mustEvictCleanPageWithoutFlushing() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier<RecordingPageCursorTracer> cursorTracerSupplier =
                new ConfigurablePageCursorTracerSupplier<>( cursorTracer );

        try ( MuninnPageCache pageCache = createPageCache( fs, 2, blockCacheFlush( tracer ), cursorTracerSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
            }
            cursorTracer.reportEvents();
            assertNotNull( cursorTracer.observe( Fault.class ) );
            assertEquals( 1, cursorTracer.faults() );
            assertEquals( 1, tracer.faults() );

            long clockArm = pageCache.evictPages( 1, 1, tracer.beginPageEvictions( 1 ) );
            assertThat( clockArm, is( 1L ) );
            assertNotNull( tracer.observe( Evict.class ) );
        }
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingFirstPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier<RecordingPageCursorTracer> cursorTracerSupplier =
                new ConfigurablePageCursorTracerSupplier<>( cursorTracer );

        try ( MuninnPageCache pageCache = createPageCache( fs, 2, blockCacheFlush( tracer ), cursorTracerSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 0L );
            }
            cursorTracer.reportEvents();
            assertNotNull( cursorTracer.observe( Fault.class ) );
            assertEquals( 1, cursorTracer.faults() );
            assertEquals( 1, tracer.faults() );

            long clockArm = pageCache.evictPages( 1, 0, tracer.beginPageEvictions( 1 ) );
            assertThat( clockArm, is( 1L ) );
            assertNotNull( tracer.observe( Evict.class ) );

            ByteBuffer buf = readIntoBuffer( "a" );
            assertThat( buf.getLong(), is( 0L ) );
            assertThat( buf.getLong(), is( y ) );
        }
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingLastPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier<RecordingPageCursorTracer> cursorTracerSupplier =
                new ConfigurablePageCursorTracerSupplier<>( cursorTracer );

        try ( MuninnPageCache pageCache = createPageCache( fs, 2, blockCacheFlush( tracer ), cursorTracerSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            try ( PageCursor cursor = pagedFile.io( 1, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 0L );
            }
            cursorTracer.reportEvents();
            assertNotNull( cursorTracer.observe( Fault.class ) );
            assertEquals( 1, cursorTracer.faults() );
            assertEquals( 1, tracer.faults() );

            long clockArm = pageCache.evictPages( 1, 0, tracer.beginPageEvictions( 1 ) );
            assertThat( clockArm, is( 1L ) );
            assertNotNull( tracer.observe( Evict.class ) );

            ByteBuffer buf = readIntoBuffer( "a" );
            assertThat( buf.getLong(), is( x ) );
            assertThat( buf.getLong(), is( 0L ) );
        }
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingAllPages() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer( Fault.class );
        ConfigurablePageCursorTracerSupplier<RecordingPageCursorTracer> cursorTracerSupplier =
                new ConfigurablePageCursorTracerSupplier<>( cursorTracer );

        try ( MuninnPageCache pageCache = createPageCache( fs, 4, blockCacheFlush( tracer ), cursorTracerSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK | PF_NO_GROW ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 0L );
                assertTrue( cursor.next() );
                cursor.putLong( 0L );
                assertFalse( cursor.next() );
            }
            cursorTracer.reportEvents();
            assertNotNull( cursorTracer.observe( Fault.class ) );
            assertNotNull( cursorTracer.observe( Fault.class ) );
            assertEquals( 2, cursorTracer.faults() );
            assertEquals( 2, tracer.faults() );

            long clockArm = pageCache.evictPages( 2, 0, tracer.beginPageEvictions( 2 ) );
            assertThat( clockArm, is( 2L ) );
            assertNotNull( tracer.observe( Evict.class ) );
            assertNotNull( tracer.observe( Evict.class ) );

            ByteBuffer buf = readIntoBuffer( "a" );
            assertThat( buf.getLong(), is( 0L ) );
            assertThat( buf.getLong(), is( 0L ) );
        }
    }

    @Test
    public void trackPageModificationTransactionId() throws Exception
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 0 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 7 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 1 );
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals( 7, pageCursor.pagedFile.getLastModifiedTxId( pageCursor.pinnedPageRef ) );
                assertEquals( 1, cursor.getLong() );
            }
        }
    }

    @Test
    public void pageModificationTrackingNoticeWriteFromAnotherThread() throws Exception
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 0 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 7 );

            Future<?> future = executor.submit( () ->
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    cursor.putLong( 1 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            future.get();

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals( 7, pageCursor.pagedFile.getLastModifiedTxId( pageCursor.pinnedPageRef ) );
                assertEquals( 1, cursor.getLong() );
            }
        }
    }

    @Test
    public void pageModificationTracksHighestModifierTransactionId() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 0 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 1 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 1 );
            }
            cursorContext.initWrite( 12 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 2 );
            }
            cursorContext.initWrite( 7 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 3 );
            }

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                assertEquals( 12, pageCursor.pagedFile.getLastModifiedTxId( pageCursor.pinnedPageRef ) );
                assertEquals( 3, cursor.getLong() );
            }
        }
    }

    @Test
    public void markCursorContextDirtyWhenRepositionCursorOnItsCurrentPage() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 3 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initRead();
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next( 0 ) );
                assertFalse( cursorContext.isDirty() );

                MuninnPageCursor pageCursor = (MuninnPageCursor) cursor;
                pageCursor.pagedFile.setLastModifiedTxId( ((MuninnPageCursor) cursor).pinnedPageRef, 17 );

                assertTrue( cursor.next( 0 ) );
                assertTrue( cursorContext.isDirty() );
            }
        }
    }

    @Test
    public void markCursorContextAsDirtyWhenReadingDataFromMoreRecentTransactions() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 3 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 7 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 3 );
            }

            cursorContext.initRead();
            assertFalse( cursorContext.isDirty() );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertEquals( 3, cursor.getLong() );
                assertTrue( cursorContext.isDirty() );
            }
        }
    }

    @Test
    public void doNotMarkCursorContextAsDirtyWhenReadingDataFromOlderTransactions() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 23 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 17 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 3 );
            }

            cursorContext.initRead();
            assertFalse( cursorContext.isDirty() );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertEquals( 3, cursor.getLong() );
                assertFalse( cursorContext.isDirty() );
            }
        }
    }

    @Test
    public void markContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionHigherThenReader() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 5 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 3 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 3 );
            }

            cursorContext.initWrite( 13 );
            try ( PageCursor cursor = pagedFile.io( 1, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 4 );
            }

            evictAllPages( pageCache );

            cursorContext.initRead();
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertEquals( 3, cursor.getLong() );
                assertTrue( cursorContext.isDirty() );
            }
        }
    }

    @Test
    public void doNotMarkContextAsDirtyWhenAnyEvictedPageHaveModificationTransactionLowerThenReader() throws IOException
    {
        TestVersionContext cursorContext = new TestVersionContext( () -> 15 );
        VersionContextSupplier versionContextSupplier = new ConfiguredVersionContextSupplier( cursorContext );
        try ( MuninnPageCache pageCache = createPageCache( fs, 2, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL,
                versionContextSupplier );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            cursorContext.initWrite( 3 );
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 3 );
            }

            cursorContext.initWrite( 13 );
            try ( PageCursor cursor = pagedFile.io( 1, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                cursor.putLong( 4 );
            }

            evictAllPages( pageCache );

            cursorContext.initRead();
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
            {
                assertTrue( cursor.next() );
                assertEquals( 3, cursor.getLong() );
                assertFalse( cursorContext.isDirty() );
            }
        }
    }

    @Test
    public void closingTheCursorMustUnlockModifiedPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );

        try ( MuninnPageCache pageCache = createPageCache( fs, 2,
                PageCacheTracer.NULL, PageCursorTracerSupplier.NULL );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            Future<?> task = executor.submit( () ->
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    cursor.putLong( 41 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            task.get();

            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() );
                long value = cursor.getLong();
                cursor.setOffset( 0 );
                cursor.putLong( value + 1 );
            }

            long clockArm = pageCache.evictPages( 1, 0, EvictionRunEvent.NULL );
            assertThat( clockArm, is( 1L ) );

            ByteBuffer buf = readIntoBuffer( "a" );
            assertThat( buf.getLong(), is( 42L ) );
            assertThat( buf.getLong(), is( y ) );
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustUnblockPageFaultersWhenEvictionGetsException() throws Exception
    {
        writeInitialDataTo( file( "a" ) );

        MutableBoolean throwException = new MutableBoolean( true );
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, openMode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        if ( throwException.booleanValue() )
                        {
                            throw new IOException( "uh-oh..." );
                        }
                        else
                        {
                            super.writeAll( src, position );
                        }
                    }
                };
            }
        };

        try ( MuninnPageCache pageCache = createPageCache( fs, 2,
                PageCacheTracer.NULL, PageCursorTracerSupplier.NULL );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            // The basic idea is that this loop, which will encounter a lot of page faults, must not block forever even
            // though the eviction thread is unable to flush any dirty pages because the file system throws
            // exceptions on all writes.
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                for ( int i = 0; i < 1000; i++ )
                {
                    assertTrue( cursor.next() );
                }
                fail( "Expected an exception at this point" );
            }
            catch ( IOException ignore )
            {
                // Good.
            }

            throwException.setFalse();
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void pageCacheFlushAndForceMustClearBackgroundEvictionException() throws Exception
    {
        MutableBoolean throwException = new MutableBoolean( true );
        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, openMode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        if ( throwException.booleanValue() )
                        {
                            throw new IOException( "uh-oh..." );
                        }
                        else
                        {
                            super.writeAll( src, position );
                        }
                    }
                };
            }
        };

        try ( MuninnPageCache pageCache = createPageCache( fs, 2,
                PageCacheTracer.NULL, PageCursorTracerSupplier.NULL );
              PagedFile pagedFile = pageCache.map( file( "a" ), 8 ) )
        {
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                assertTrue( cursor.next() ); // Page 0 is now dirty, but flushing it will throw an exception.
            }

            // This will run into that exception, in background eviction:
            pageCache.evictPages( 1, 0, EvictionRunEvent.NULL );

            // We now have a background eviction exception. A successful flushAndForce should clear it, though.
            throwException.setFalse();
            pageCache.flushAndForce();

            // And with a cleared exception, we should be able to work with the page cache without worry.
            try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
            {
                for ( int i = 0; i < maxPages * 20; i++ )
                {
                    assertTrue( cursor.next() );
                }
            }
        }
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustThrowIfMappingFileWouldOverflowReferenceCount() throws Exception
    {
        File file = file( "a" );
        writeInitialDataTo( file );
        try ( MuninnPageCache pageCache = createPageCache( fs, 30, PageCacheTracer.NULL,
                DefaultPageCursorTracerSupplier.NULL ) )
        {
            PagedFile pf = null;
            int i = 0;

            try
            {
                expectedException.expect( IllegalStateException.class );
                for ( ; i < Integer.MAX_VALUE; i++ )
                {
                    pf = pageCache.map( file, filePageSize );
                }
            }
            finally
            {
                for ( int j = 0; j < i; j++ )
                {
                    try
                    {
                        pf.close();
                    }
                    catch ( Exception e )
                    {
                        //noinspection ThrowFromFinallyBlock
                        throw new AssertionError( "Did not expect pf.close() to throw", e );
                    }
                }
            }
        }
    }

    private void evictAllPages( MuninnPageCache pageCache ) throws IOException
    {
        PageList pages = pageCache.pages;
        for ( int pageId = 0; pageId < pages.getPageCount(); pageId++ )
        {
            long pageReference = pages.deref( pageId );
            while ( pages.isLoaded( pageReference ) )
            {
                pages.tryEvict( pageReference, EvictionRunEvent.NULL );
            }
        }
        for ( int pageId = 0; pageId < pages.getPageCount(); pageId++ )
        {
            long pageReference = pages.deref( pageId );
            pageCache.addFreePageToFreelist( pageReference );
        }
    }

    private void writeInitialDataTo( File file ) throws IOException
    {
        try ( StoreChannel channel = fs.create( file ) )
        {
            ByteBuffer buf = ByteBuffer.allocate( 16 );
            buf.putLong( x );
            buf.putLong( y );
            buf.flip();
            channel.writeAll( buf );
        }
    }

    private ByteBuffer readIntoBuffer( String fileName ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( 16 );
        try ( StoreChannel channel = fs.open( file( fileName ), OpenMode.READ ) )
        {
            channel.readAll( buffer );
        }
        buffer.flip();
        return buffer;
    }

    private static class ConfiguredVersionContextSupplier implements VersionContextSupplier
    {

        private final VersionContext versionContext;

        ConfiguredVersionContextSupplier( VersionContext versionContext )
        {
            this.versionContext = versionContext;
        }

        @Override
        public void init( LongSupplier lastClosedTransactionIdSupplier )
        {
        }

        @Override
        public VersionContext getVersionContext()
        {
            return versionContext;
        }
    }

    private static class TestVersionContext implements VersionContext
    {

        private final IntSupplier closedTxIdSupplier;
        private long committingTxId;
        private long lastClosedTxId;
        private boolean dirty;

        TestVersionContext( IntSupplier closedTxIdSupplier )
        {
            this.closedTxIdSupplier = closedTxIdSupplier;
        }

        @Override
        public void initRead()
        {
            this.lastClosedTxId = closedTxIdSupplier.getAsInt();
        }

        @Override
        public void initWrite( long committingTxId )
        {
            this.committingTxId = committingTxId;
        }

        @Override
        public long committingTransactionId()
        {
            return committingTxId;
        }

        @Override
        public long lastClosedTransactionId()
        {
            return lastClosedTxId;
        }

        @Override
        public void markAsDirty()
        {
            dirty = true;
        }

        @Override
        public boolean isDirty()
        {
            return dirty;
        }
    }
}
