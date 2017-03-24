/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
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
    public void mustEvictCleanPageWithoutFlushing() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier cursorTracerSupplier = new ConfigurablePageCursorTracerSupplier( cursorTracer );

        MuninnPageCache pageCache = createPageCache( fs, 2, 8, blockCacheFlush( tracer ), cursorTracerSupplier );
        PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_READ_LOCK ) )
        {
            assertTrue( cursor.next() );
        }
        cursorTracer.reportEvents();
        assertNotNull( cursorTracer.observe( Fault.class ) );
        assertEquals( 1, cursorTracer.faults() );
        assertEquals( 1, tracer.faults() );

        int clockArm = pageCache.evictPages( 1, 0, tracer.beginPageEvictions( 1 ) );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( tracer.observe( Evict.class ) );
    }

    private void writeInitialDataTo( File file ) throws IOException
    {
        StoreChannel channel = fs.create( file );
        ByteBuffer buf = ByteBuffer.allocate( 16 );
        buf.putLong( x );
        buf.putLong( y );
        buf.flip();
        channel.writeAll( buf );
        channel.close();
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingFirstPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier cursorTracerSupplier = new ConfigurablePageCursorTracerSupplier( cursorTracer );

        MuninnPageCache pageCache = createPageCache( fs, 2, 8, blockCacheFlush( tracer ),
                cursorTracerSupplier );
        PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
        }
        cursorTracer.reportEvents();
        assertNotNull( cursorTracer.observe( Fault.class ) );
        assertEquals( 1, cursorTracer.faults() );
        assertEquals( 1, tracer.faults() );

        int clockArm = pageCache.evictPages( 1, 0, tracer.beginPageEvictions( 1 ) );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( tracer.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file( "a" ), "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 0L ) );
        assertThat( buf.getLong(), is( y ) );
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingLastPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer();
        ConfigurablePageCursorTracerSupplier cursorTracerSupplier = new ConfigurablePageCursorTracerSupplier( cursorTracer );

        MuninnPageCache pageCache = createPageCache( fs, 2, 8, blockCacheFlush( tracer ),
                cursorTracerSupplier );
        PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

        try ( PageCursor cursor = pagedFile.io( 1, PF_SHARED_WRITE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
        }
        cursorTracer.reportEvents();
        assertNotNull( cursorTracer.observe( Fault.class ) );
        assertEquals( 1, cursorTracer.faults() );
        assertEquals( 1, tracer.faults() );

        int clockArm = pageCache.evictPages( 1, 0, tracer.beginPageEvictions( 1 ) );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( tracer.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file( "a" ), "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( x ) );
        assertThat( buf.getLong(), is( 0L ) );
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingAllPages() throws Exception
    {
        writeInitialDataTo( file( "a" ) );
        RecordingPageCacheTracer tracer = new RecordingPageCacheTracer();
        RecordingPageCursorTracer cursorTracer = new RecordingPageCursorTracer( Fault.class );
        ConfigurablePageCursorTracerSupplier cursorTracerSupplier = new ConfigurablePageCursorTracerSupplier( cursorTracer );

        MuninnPageCache pageCache = createPageCache( fs, 4, 8, blockCacheFlush( tracer ),
                cursorTracerSupplier );
        PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

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

        int clockArm = pageCache.evictPages( 2, 0, tracer.beginPageEvictions( 2 ) );
        assertThat( clockArm, is( 2 ) );
        assertNotNull( tracer.observe( Evict.class ) );
        assertNotNull( tracer.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file( "a" ), "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 0L ) );
        assertThat( buf.getLong(), is( 0L ) );
    }

    @Test
    public void closingTheCursorMustUnlockModifiedPage() throws Exception
    {
        writeInitialDataTo( file( "a" ) );

        final MuninnPageCache pageCache = createPageCache( fs, 2, 8, PageCacheTracer.NULL,
                DefaultPageCursorTracerSupplier.INSTANCE );
        final PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

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

        int clockArm = pageCache.evictPages( 1, 0, EvictionRunEvent.NULL );
        assertThat( clockArm, is( 1 ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file( "a" ), "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 42L ) );
        assertThat( buf.getLong(), is( y ) );
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustUnblockPageFaultersWhenEvictionGetsException() throws Exception
    {
        writeInitialDataTo( file( "a" ) );

        FileSystemAbstraction fs = new DelegatingFileSystemAbstraction( this.fs )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public void writeAll( ByteBuffer src, long position ) throws IOException
                    {
                        throw new IOException( "uh-oh..." );
                    }
                };
            }
        };

        MuninnPageCache pageCache = createPageCache( fs, 2, 8, PageCacheTracer.NULL,
                DefaultPageCursorTracerSupplier.INSTANCE );
        final PagedFile pagedFile = pageCache.map( file( "a" ), 8 );

        // The basic idea is that this loop, which will encounter a lot of page faults, must not block forever even
        // though the eviction thread is unable to flush any dirty pages because the file system throws exceptions on
        // all writes.
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
    }

    @Test( timeout = SEMI_LONG_TIMEOUT_MILLIS )
    public void mustThrowIfMappingFileWouldOverflowReferenceCount() throws Exception
    {
        File file = file( "a" );
        writeInitialDataTo( file );
        MuninnPageCache pageCache = createPageCache( fs, 30, pageCachePageSize, PageCacheTracer.NULL,
                DefaultPageCursorTracerSupplier.NULL );
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
