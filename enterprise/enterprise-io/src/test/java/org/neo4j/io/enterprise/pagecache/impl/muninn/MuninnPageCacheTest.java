/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.enterprise.pagecache.impl.muninn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheMonitor;
import org.neo4j.io.pagecache.PageCacheTest;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.RecordingPageCacheMonitor;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.io.pagecache.RecordingPageCacheMonitor.Evict;
import static org.neo4j.io.pagecache.RecordingPageCacheMonitor.Fault;

public class MuninnPageCacheTest extends PageCacheTest<MuninnPageCache>
{
    static {
        // This is disabled by default, but we have tests that verify that
        // pinned and unpinned are called correctly.
        // Setting this property here in the test class should ensure that
        // it is set before the MuninnPageCache classes are loaded, and
        // thus before they check this value.
        System.setProperty(
                "org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.monitorPinUnpin", "true" );
    }

    private static final ConcurrentMap<PageCache, Future<?>> futures = new ConcurrentHashMap<>();
    private final long x = 0xCAFEBABEDEADBEEFL;
    private final long y = 0xDECAFC0FFEEDECAFL;

    @Override
    protected MuninnPageCache createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor )
    {
        MuninnPageCache pageCache = new MuninnPageCache( fs, maxPages, pageSize, monitor );
        Future<?> future = executor.submit( pageCache );
        futures.put( pageCache, future );
        return pageCache;
    }

    @Override
    protected void tearDownPageCache( MuninnPageCache pageCache ) throws IOException
    {
        pageCache.close();
        Future<?> future = futures.remove( pageCache );
        if ( future != null )
        {
            future.cancel( true );
        }
    }

    @Test
    public void primitiveLongIntMapReturnsMinusOneForKeysNotFound()
    {
        PrimitiveLongIntMap map = Primitive.longIntMap( 5 );
        assertThat( map.get( 42 ), is( -1 ) );
    }

    @Test
    public void mustEvictCleanPageWithoutFlushing() throws Exception
    {
        writeInitialDataTo( file );
        RecordingPageCacheMonitor monitor = new RecordingPageCacheMonitor();

        MuninnPageCache pageCache = new MuninnPageCache( fs, 2, 8, monitor );
        PagedFile pagedFile = pageCache.map( file, 8 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            assertTrue( cursor.next() );
        }
        assertNotNull( monitor.observe( Fault.class ) );

        int clockArm = pageCache.evictPages( 1, 0 );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( monitor.observe( Evict.class ) );
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
        writeInitialDataTo( file );
        RecordingPageCacheMonitor monitor = new RecordingPageCacheMonitor();

        MuninnPageCache pageCache = new MuninnPageCache( fs, 2, 8, monitor );
        PagedFile pagedFile = pageCache.map( file, 8 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
        }
        assertNotNull( monitor.observe( Fault.class ) );

        int clockArm = pageCache.evictPages( 1, 0 );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( monitor.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file, "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 0L ) );
        assertThat( buf.getLong(), is( y ) );
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingLastPage() throws Exception
    {
        writeInitialDataTo( file );
        RecordingPageCacheMonitor monitor = new RecordingPageCacheMonitor();

        MuninnPageCache pageCache = new MuninnPageCache( fs, 2, 8, monitor );
        PagedFile pagedFile = pageCache.map( file, 8 );

        try ( PageCursor cursor = pagedFile.io( 1, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
        }
        assertNotNull( monitor.observe( Fault.class ) );

        int clockArm = pageCache.evictPages( 1, 0 );
        assertThat( clockArm, is( 1 ) );
        assertNotNull( monitor.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file, "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( x ) );
        assertThat( buf.getLong(), is( 0L ) );
    }

    @Test
    public void mustFlushDirtyPagesOnEvictingAllPages() throws Exception
    {
        writeInitialDataTo( file );
        RecordingPageCacheMonitor monitor = new RecordingPageCacheMonitor();

        MuninnPageCache pageCache = new MuninnPageCache( fs, 2, 8, monitor );
        PagedFile pagedFile = pageCache.map( file, 8 );

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
            assertTrue( cursor.next() );
            cursor.putLong( 0L );
            assertFalse( cursor.next() );
        }
        assertNotNull( monitor.observe( Fault.class ) );
        assertNotNull( monitor.observe( Fault.class ) );

        int clockArm = pageCache.evictPages( 2, 0 );
        assertThat( clockArm, is( 2 ) );
        assertNotNull( monitor.observe( Evict.class ) );
        assertNotNull( monitor.observe( Evict.class ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file, "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 0L ) );
        assertThat( buf.getLong(), is( 0L ) );
    }

    @Test
    public void closingTheCursorMustUnlockModifiedPage() throws Exception
    {
        writeInitialDataTo( file );

        final MuninnPageCache pageCache = new MuninnPageCache( fs, 2, 8, PageCacheMonitor.NULL );
        final PagedFile pagedFile = pageCache.map( file, 8 );

        Future<?> task = executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
                {
                    assertTrue( cursor.next() );
                    cursor.putLong( 41 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
        task.get();

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            long value = cursor.getLong();
            cursor.setOffset( 0 );
            cursor.putLong( value + 1 );
        }

        int clockArm = pageCache.evictPages( 1, 0 );
        assertThat( clockArm, is( 1 ) );

        ByteBuffer buf = ByteBuffer.allocate( 16 );
        StoreChannel channel = fs.open( file, "r" );
        channel.read( buf );
        buf.flip();
        assertThat( buf.getLong(), is( 42L ) );
        assertThat( buf.getLong(), is( y ) );
    }
}
