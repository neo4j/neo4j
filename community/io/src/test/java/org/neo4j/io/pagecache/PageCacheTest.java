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
package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.common.Page;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

public abstract class PageCacheTest<T extends PageCache>
{
    private final File file = new File( "a" );

    private int recordSize = 9;
    private int recordCount = 1060;
    private int maxPages = 20;
    private int pageCachePageSize = 20;
    private int filePageSize = 18;
    private int recordsPerFilePage = filePageSize / recordSize;

    private EphemeralFileSystemAbstraction fs;

    protected abstract T createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor );

    protected abstract void tearDownPageCache( T pageCache ) throws IOException;

    private T pageCache;

    protected final T getPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheMonitor monitor ) throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        pageCache = createPageCache( fs, maxPages, pageSize, monitor );
        return pageCache;
    }

    @Before
    public void setUp()
    {
        fs = new EphemeralFileSystemAbstraction();
    }

    @After
    public void tearDown() throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        fs.shutdown();
    }

    private static class RecordPageIO implements PageIO
    {
        /** Verify that the contents of all the records are what we expect for the given pages. */
        public static final long IO_FLAGS_VERIFY_RECORDS = 1;
        public static final long IO_FLAGS_WRITE_RECORDS = 2;
        public static final long IO_FLAGS_CORRUPT_RECORDS = 3;
        public int recordSize;
        public int recordsPerFilePage;
        public ByteBuffer bufA;
        public ByteBuffer bufB;
        public long pageId;
        public Page page;

        public RecordPageIO( int recordSize, int recordsPerFilePage )
        {
            bufA = ByteBuffer.allocate( recordSize );
            bufB = ByteBuffer.allocate( recordSize );
            this.recordSize = recordSize;
            this.recordsPerFilePage = recordsPerFilePage;
        }

        @Override
        public void apply( long pageId, Page page, long io_context, long io_flags )
        {
            if ( io_flags == IO_FLAGS_VERIFY_RECORDS )
            {
                verifyRecordsMatchExpected( pageId, page );
            }
            else if ( io_flags == IO_FLAGS_WRITE_RECORDS )
            {
                writeRecords( pageId, page );
            }
            else if ( io_flags == IO_FLAGS_CORRUPT_RECORDS )
            {
                corruptRecords( page );
            }

            this.pageId = pageId;
            this.page = page;
        }

        private void verifyRecordsMatchExpected( long pageId, Page page )
        {
            for ( int i = 0; i < recordsPerFilePage; i++ )
            {
                long recordId = (pageId * recordsPerFilePage) + i;
                generateRecordForId( recordId, bufA );
                page.getBytes( bufB.array(), i * recordSize );
                assertThat( "Record id: " + recordId, bufB.array(), byteArray( bufA.array() ) );
            }
        }

        private void writeRecords( long pageId, Page page )
        {
            for ( int i = 0; i < recordsPerFilePage; i++ )
            {
                long recordId = (pageId * recordsPerFilePage) + i;
                generateRecordForId( recordId, bufA );
                page.putBytes( bufA.array(), i * recordSize );
            }
        }

        private void corruptRecords( Page page )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            byte[] bytes = new byte[ recordSize * recordsPerFilePage ];
            rng.nextBytes( bytes );
            page.putBytes( bytes, 0 );
        }
    }

    @Test
    public void mustReadExistingData() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );
        int recordId = 0;
        try ( PageCursor cursor = pagedFile.io(
                0L,
                PF_SHARED_LOCK | PF_NO_GROW,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            while ( cursor.next() )
            {
                recordId += recordsPerFilePage;
            }
        }

        assertThat( recordId, is( recordCount ) );
    }

    private void generateFileWithRecords(
            File file,
            int recordCount,
            int recordSize ) throws IOException
    {
        StoreChannel channel = fs.open( file, "w" );
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            channel.writeAll( buf );
        }
        channel.close();
    }

    @Test
    public void mustScanInTheMiddleOfTheFile() throws IOException
    {
        long startPage = 10;
        long endPage = (recordCount / recordsPerFilePage) - 10;
        generateFileWithRecords( file, recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );
        int recordId = (int) (startPage * recordsPerFilePage);
        try ( PageCursor cursor = pagedFile.io(
                startPage,
                PF_SHARED_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            while ( cursor.next() && pageIO.pageId < endPage )
            {
                recordId += recordsPerFilePage;
            }
        }

        assertThat( recordId, is( recordCount - (10 * recordsPerFilePage) ) );
    }

    @Test
    public void writesFlushedFromPageFileMustBeExternallyObservable() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io(
                startPageId,
                PF_EXCLUSIVE_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_WRITE_RECORDS ) )
        {
            while ( pageIO.pageId < endPageId && cursor.next() );
        }

        pagedFile.flush();

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertThat( "Record id: " + i, observation.array(), byteArray( buf.array() ) );
        }
        channel.close();
    }

    @Test
    public void writesFlushedFromPageCacheMustBeExternallyObservable() throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io(
                startPageId,
                PF_EXCLUSIVE_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_WRITE_RECORDS ) )
        {
            while ( pageIO.pageId < endPageId && cursor.next() );
        }

        cache.flush();

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertThat( "Record id: " + i, observation.array(), byteArray( buf.array() ) );
        }
        channel.close();
    }

    @Test
    public void firstNextCallMustReturnFalseWhenTheFileIsEmptyAndNoGrowIsSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        try ( PageCursor cursor = pagedFile.io(
                0,
                PF_SHARED_LOCK | PF_NO_GROW,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void nextMustReturnTrueThenFalseWhenThereIsOnlyOnePageInTheFileAndNoGrowIsSpecified() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        try ( PageCursor cursor = pagedFile.io(
                0,
                PF_SHARED_LOCK | PF_NO_GROW,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }
    }

    @Test( timeout = 1000 )
    public void closingWithoutCallingNextMustLeavePageUnpinnedAndUntouched() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        try ( PageCursor ignore = pagedFile.io(
                0,
                PF_EXCLUSIVE_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_CORRUPT_RECORDS ) )
        {
            // No call to next, so the page should never get pinned in the first place, nor
            // should the page corruption take place.
        }

        try ( PageCursor cursor = pagedFile.io(
                0,
                PF_SHARED_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            // We didn't call next before, so the page and its records should still be fine
            cursor.next();
        }
    }

    @Test
    public void rewindMustStartScanningOverFromTheBeginning() throws IOException
    {
        int numberOfRewindsToTest = 10;
        generateFileWithRecords( file, recordCount, recordSize );
        int actualPageCounter = 0;
        int filePageCount = recordCount / recordsPerFilePage;
        int expectedPageCounterResult = numberOfRewindsToTest * filePageCount;

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        try ( PageCursor cursor = pagedFile.io(
                0,
                PF_SHARED_LOCK | PF_NO_GROW,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            for ( int i = 0; i < numberOfRewindsToTest; i++ )
            {
                while ( cursor.next() )
                {
                    actualPageCounter++;
                }
                cursor.rewind();
            }
        }

        assertThat( actualPageCounter, is( expectedPageCounterResult ) );
    }

    @Test( timeout = 1000 )
    public void exceptionFromApplyingPageIOMustLeavePageUnpinned() throws IOException
    {
        int numberOfRecordsToGenerate = recordsPerFilePage; // one page worth
        generateFileWithRecords( file, numberOfRecordsToGenerate, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        final RuntimeException exception = new RuntimeException( "boo!" );
        PageIO pageIO = new PageIO()
        {
            @Override
            public void apply( long pageId, Page page, long io_context, long io_flags ) throws IOException
            {
                throw exception;
            }
        };

        try ( PageCursor cursor = pagedFile.io( 0, PF_EXCLUSIVE_LOCK, pageIO, 0, 0 ) )
        {
            cursor.next();
            fail( "Call to next() should have thrown" );
        }
        catch ( RuntimeException e )
        {
            assertThat( e, sameInstance( exception ) );
        }

        // Now that the exception has bubbled out, the page must no longer be pinned

        pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        try ( PageCursor cursor = pagedFile.io(
                0,
                PF_SHARED_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_VERIFY_RECORDS ) )
        {
            cursor.next();
        }
    }

    @Test
    public void mustCloseFileChannelWhenTheLastHandleIsUnmapped() throws Exception
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, filePageSize );
        cache.map( file, filePageSize );
        cache.unmap( file );
        cache.unmap( file );
        fs.assertNoOpenFiles();
    }

    @Test
    public void dirtyPagesMustBeFlushedWhenTheCacheIsClosed() throws IOException
    {

        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        RecordPageIO pageIO = new RecordPageIO( recordSize, recordsPerFilePage );

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io(
                startPageId,
                PF_EXCLUSIVE_LOCK,
                pageIO,
                0,
                RecordPageIO.IO_FLAGS_WRITE_RECORDS ) )
        {
            while ( pageIO.pageId < endPageId && cursor.next() );
        }

        cache.close();

        StoreChannel channel = fs.open( file, "r" );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertThat( "Record id: " + i, observation.array(), byteArray( buf.array() ) );
        }
        channel.close();
    }
    // TODO must unmap all files when the cache is closed
    // TODO mapping files in a closed cache must throw
    // TODO must collect all exceptions from closing file channels when the cache is closed
    // TODO scanning with concurrent writes

    private static void generateRecordForId( long id, ByteBuffer buf )
    {
        buf.position( 0 );
        int x = (int) (id + 1);
        buf.putInt( x );
        while ( buf.position() < buf.limit() )
        {
            x++;
            buf.put( (byte) (x & 0xFF) );
        }
        buf.position( 0 );
    }

    @Ignore( "A meta-test that verifies that the byteArray matcher works as expected." +
             "Ignored because it is intentionally failing." )
    @Test
    public void metatestForByteArrayMatcher()
    {
        byte[] a = new byte[] { 1, -2, 3 };
        byte[] b = new byte[] { 1, 3, -2 };
        assertThat( "a == a", a, byteArray( a ) );
        assertThat( "a != b", a, byteArray( b ) ); // this must fail
    }

    private static Matcher<byte[]> byteArray( final byte[] expected )
    {
        return new TypeSafeDiagnosingMatcher<byte[]>()
        {
            @Override
            protected boolean matchesSafely( byte[] actual, Description description )
            {
                describe( actual, description );
                if ( actual.length != expected.length )
                {
                    return false;
                }
                for ( int i = 0; i < expected.length; i++ )
                {
                    if ( actual[i] != expected[i] )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                describe( expected, description );
            }

            private void describe( byte[] bytes, Description description )
            {
                description.appendText( "byte[] { " );
                for ( int i = 0; i < bytes.length; i++ )
                {
                    int b = bytes[i] & 0xFF;
                    description.appendText( String.format( "%02X ", b ) );
                }
                description.appendText( "}" );
            }
        };
    }
}
