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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SINGLE_PAGE;

public abstract class PageCacheTest<T extends PageCache>
{
    protected final File file = new File( "a" );

    protected int recordSize = 9;
    protected int recordCount = 1060;
    protected int maxPages = 20;
    protected int pageCachePageSize = 20;
    protected int filePageSize = 18;
    protected int recordsPerFilePage = filePageSize / recordSize;
    protected ByteBuffer bufA = ByteBuffer.allocate( recordSize );
    protected ByteBuffer bufB = ByteBuffer.allocate( recordSize );

    protected EphemeralFileSystemAbstraction fs;

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

    private void verifyRecordsMatchExpected( PageCursor cursor )
    {
        long pageId = cursor.getCurrentPageId();
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (pageId * recordsPerFilePage) + i;
            generateRecordForId( recordId, bufA );
            cursor.getBytes( bufB.array() );
            assertThat( "Record id: " + recordId, bufB.array(), byteArray( bufA.array() ) );
        }
    }

    private void writeRecords( PageCursor cursor )
    {
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (cursor.getCurrentPageId() * recordsPerFilePage) + i;
            generateRecordForId( recordId, bufA );
            cursor.putBytes( bufA.array() );
        }
    }

    @Test
    public void mustReadExistingData() throws IOException
    {
        generateFileWithRecords( file, recordCount, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        int recordId = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK | PF_NO_GROW ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
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

        int recordId = (int) (startPage * recordsPerFilePage);
        try ( PageCursor cursor = pagedFile.io( startPage, PF_SHARED_LOCK ) )
        {
            while ( cursor.next() && cursor.getCurrentPageId() < endPage )
            {
                verifyRecordsMatchExpected( cursor );
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

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                writeRecords( cursor );
            }
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

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                writeRecords( cursor );
            }
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

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK | PF_NO_GROW ) )
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

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            verifyRecordsMatchExpected( cursor );
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

        //noinspection EmptyTryBlock
        try ( PageCursor ignore = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            // No call to next, so the page should never get pinned in the first place, nor
            // should the page corruption take place.
        }

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            // We didn't call next before, so the page and its records should still be fine
            cursor.next();
            verifyRecordsMatchExpected( cursor );
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

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK | PF_NO_GROW ) )
        {
            for ( int i = 0; i < numberOfRewindsToTest; i++ )
            {
                while ( cursor.next() )
                {
                    verifyRecordsMatchExpected( cursor );
                    actualPageCounter++;
                }
                cursor.rewind();
            }
        }

        assertThat( actualPageCounter, is( expectedPageCounterResult ) );
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

        long startPageId = 0;
        long endPageId = recordCount / recordsPerFilePage;
        try ( PageCursor cursor = pagedFile.io( startPageId, PF_EXCLUSIVE_LOCK ) )
        {
            while ( cursor.getCurrentPageId() < endPageId && cursor.next() )
            {
                writeRecords( cursor );
            }
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

    @Test( expected = IllegalStateException.class )
    public void mappingFilesInClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.close();
        cache.map( file, filePageSize );
    }

    @Test( expected = IllegalStateException.class )
    public void flushingClosedCacheMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.close();
        cache.flush();
    }

    @Test( expected = IllegalArgumentException.class )
    public void mappingFileWithPageSizeGreaterThanCachePageSizeMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, pageCachePageSize + 1 ); // this must throw
    }

    @Test
    public void mappingFileWithPageSizeEqualToCachePageSizeMustNotThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        cache.map( file, pageCachePageSize );// this must NOT throw
        cache.unmap( file );
    }

    @Test
    public void nextMustReturnTrueThenFalseWhenSinglePageIsSpecified() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK | PF_SINGLE_PAGE ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            assertFalse( cursor.next() );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void specifyingBothReadAheadAndSinglePageMustThrow() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );
        pagedFile.io( 0, PF_SHARED_LOCK | PF_READ_AHEAD | PF_SINGLE_PAGE ); // this must throw
    }

    @Test( timeout = 1000 )
    public void mustNotPinPagesAfterNextReturnsFalse() throws Exception
    {
        final CountDownLatch startLatch = new CountDownLatch( 1 );
        final CountDownLatch unpinLatch = new CountDownLatch( 1 );
        final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        final PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        final PagedFile pagedFile = cache.map( file, filePageSize );

        Runnable runnable = new Runnable()
        {
            @Override
            public void run()
            {
                try ( PageCursor cursorA = pagedFile.io( 0, PF_EXCLUSIVE_LOCK | PF_SINGLE_PAGE ) )
                {
                    assertTrue( cursorA.next() );
                    assertFalse( cursorA.next() );
                    startLatch.countDown();
                    unpinLatch.await();
                    cursorA.close();
                }
                catch ( Exception e )
                {
                    exceptionRef.set( e );
                }
            }
        };
        Thread thread = new Thread( runnable );
        thread.start();

        startLatch.await();
        try ( PageCursor cursorB = pagedFile.io( 1, PF_EXCLUSIVE_LOCK | PF_SINGLE_PAGE ) )
        {
            assertTrue( cursorB.next() );
            assertFalse( cursorB.next() );
        }
        unpinLatch.countDown();
        Exception e = exceptionRef.get();
        if ( e != null )
        {
            throw new Exception( "Child thread got exception", e );
        }
    }

    @Test
    public void nextMustResetTheCursorOffset() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            cursor.setOffset( 0 );
            cursor.putByte( (byte) 1 );
            cursor.putByte( (byte) 2 );
            cursor.putByte( (byte) 3 );
            cursor.putByte( (byte) 4 );
            assertTrue( cursor.next() );
            cursor.setOffset( 0 );
            cursor.putByte( (byte) 5 );
            cursor.putByte( (byte) 6 );
            cursor.putByte( (byte) 7 );
            cursor.putByte( (byte) 8 );
        }

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getByte(), is((byte) 1 ) );
            assertThat( cursor.getByte(), is((byte) 2 ) );
            assertThat( cursor.getByte(), is((byte) 3 ) );
            assertThat( cursor.getByte(), is((byte) 4 ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getByte(), is((byte) 5 ) );
            assertThat( cursor.getByte(), is((byte) 6 ) );
            assertThat( cursor.getByte(), is((byte) 7 ) );
            assertThat( cursor.getByte(), is((byte) 8 ) );
        }
    }

    @Test
    public void nextMustAdvanceCurrentPageId() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 1L ) );
        }
    }

    @Test
    public void currentPageIdIsUnboundBeforeFirstNextAndAfterRewind() throws IOException
    {
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK ) )
        {
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
            assertTrue( cursor.next() );
            assertThat( cursor.getCurrentPageId(), is( 0L ) );
            cursor.rewind();
            assertThat( cursor.getCurrentPageId(), is( PageCursor.UNBOUND_PAGE_ID ) );
        }
    }

    @Test
    public void lastPageMustBeAccessibleWithNoGrowSpecified() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage * 2, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 2L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 3L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void lastPageMustBeAccessibleWithNoGrowSpecifiedEvenIfLessThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file, (recordsPerFilePage * 2) - 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 2L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 3L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void firstPageMustBeAccessibleWithNoGrowSpecifiedIfItIsTheOnlyPage() throws IOException
    {
        generateFileWithRecords( file, recordsPerFilePage, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void firstPageMustBeAccessibleEvenIfTheFileIsSmallerThanFilePageSize() throws IOException
    {
        generateFileWithRecords( file, 1, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );

        try ( PageCursor cursor = pagedFile.io( 0L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertTrue( cursor.next() );
            assertFalse( cursor.next() );
        }

        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK | PF_NO_GROW ) )
        {
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void newlyWrittenPagesMustBeAccessibleWithNoGrow() throws IOException
    {
        int initialPages = 1;
        int pagesToAdd = 3;
        generateFileWithRecords( file, recordsPerFilePage * initialPages, recordSize );

        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        PagedFile pagedFile = cache.map( file, filePageSize );


        try ( PageCursor cursor = pagedFile.io( 1L, PF_EXCLUSIVE_LOCK ) )
        {
            for ( int i = 0; i < pagesToAdd; i++ )
            {
                assertTrue( cursor.next() );
                writeRecords( cursor );
            }
        }

        int pagesChecked = 0;
        try ( PageCursor cursor = pagedFile.io( 0L, PF_SHARED_LOCK | PF_NO_GROW ) )
        {
            while ( cursor.next() )
            {
                verifyRecordsMatchExpected( cursor );
                pagesChecked++;
            }
        }
        assertThat( pagesChecked, is( initialPages + pagesToAdd ) );
    }

    // TODO are we allowed to grow the file without PF_EXCLUSIVE_LOCK???
    // TODO must collect all exceptions from closing file channels when the cache is closed
    // TODO scanning with concurrent writes
    // TODO figure out what should happen when the last reference to a file is unmapped, while pages are still pinned
    // TODO figure out how closing the cache should work when there are still mapped files




    @Test
    public void shouldCloseAllFilesWhenClosingThePageCache() throws Exception
    {
        // TODO really? close files that have not been unmapped?
        // Given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        PageCache cache = getPageCache( fs, maxPages, pageCachePageSize, PageCacheMonitor.NULL );
        File file1Name = new File( "file1" );
        File file2Name = new File( "file2" );

        StoreChannel channel1 = mock( StoreChannel.class );
        StoreChannel channel2 = mock( StoreChannel.class );
        when( fs.open( file1Name, "rw" ) ).thenReturn( channel1 );
        when( fs.open( file2Name, "rw" ) ).thenReturn( channel2 );

        // When
        cache.map( file1Name, filePageSize );
        cache.map( file1Name, filePageSize );
        cache.map( file2Name, filePageSize );
        cache.unmap( file2Name );

        // Then
        verify( fs ).open( file1Name, "rw" );
        verify( fs ).open( file2Name, "rw" );
        verify( channel2, atLeast( 1 ) ).force( false );
        verify( channel2 ).close();
        verify( channel1, atLeast( 0 ) ).size();
        verify( channel2, atLeast( 0 ) ).size();
        verifyNoMoreInteractions( channel1, channel2, fs );

        // And When
        cache.close();

        // Then
        verify( channel1, atLeast( 1 ) ).force( false );
        verify( channel1 ).close();
        verifyNoMoreInteractions( channel1, channel2, fs );
    }


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
