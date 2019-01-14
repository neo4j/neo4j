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
package org.neo4j.io.pagecache;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.test.rule.RepeatRule;

import static org.junit.Assert.assertThat;
import static org.neo4j.test.matchers.ByteArrayMatcher.byteArray;

public abstract class PageCacheTestSupport<T extends PageCache>
{
    protected static final long SHORT_TIMEOUT_MILLIS = 10_000;
    protected static final long SEMI_LONG_TIMEOUT_MILLIS = 120_000;
    protected static final long LONG_TIMEOUT_MILLIS = 360_000;
    protected static ExecutorService executor;

    @BeforeClass
    public static void startExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor()
    {
        executor.shutdown();
    }

    public RepeatRule repeatRule = new RepeatRule();
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public RuleChain rules = RuleChain.outerRule( repeatRule ).around( expectedException );

    protected int recordSize = 9;
    protected int maxPages = 20;

    protected int pageCachePageSize;
    protected int recordsPerFilePage;
    protected int recordCount;
    protected int filePageSize;
    protected ByteBuffer bufA;
    protected FileSystemAbstraction fs;
    protected T pageCache;

    private Fixture<T> fixture;

    protected abstract Fixture<T> createFixture();

    @Before
    public void setUp() throws IOException
    {
        fixture = createFixture();
        Thread.interrupted(); // Clear stray interrupts
        fs = createFileSystemAbstraction();
        ensureExists( file( "a" ) );
    }

    @After
    public void tearDown() throws Exception
    {
        Thread.interrupted(); // Clear stray interrupts

        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        fs.close();
    }

    protected final T createPageCache( PageSwapperFactory swapperFactory, int maxPages,
                                       PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier,
                                       VersionContextSupplier versionContextSupplier )
    {
        T pageCache = fixture.createPageCache( swapperFactory, maxPages, tracer, cursorTracerSupplier, versionContextSupplier );
        pageCachePageSize = pageCache.pageSize();
        recordsPerFilePage = pageCachePageSize / recordSize;
        recordCount = 5 * maxPages * recordsPerFilePage;
        filePageSize = recordsPerFilePage * recordSize;
        bufA = ByteBuffer.allocate( recordSize );
        return pageCache;
    }

    protected T createPageCache( FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer,
                                 PageCursorTracerSupplier cursorTracerSupplier, VersionContextSupplier versionContextSupplier )
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.open( fs, Configuration.EMPTY );
        return createPageCache( swapperFactory, maxPages, tracer, cursorTracerSupplier, versionContextSupplier );
    }

    protected T createPageCache( FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer,
            PageCursorTracerSupplier cursorTracerSupplier )
    {
        return createPageCache( fs, maxPages, tracer, cursorTracerSupplier, EmptyVersionContextSupplier.EMPTY );
    }

    protected final T getPageCache( FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer,
                                    PageCursorTracerSupplier cursorTracerSupplier )
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        pageCache = createPageCache( fs, maxPages, tracer, cursorTracerSupplier, EmptyVersionContextSupplier.EMPTY );
        return pageCache;
    }

    protected void configureStandardPageCache()
    {
        getPageCache( fs, maxPages, PageCacheTracer.NULL, PageCursorTracerSupplier.NULL );
    }

    protected final void tearDownPageCache( T pageCache )
    {
        fixture.tearDownPageCache( pageCache );
    }

    protected final FileSystemAbstraction createFileSystemAbstraction()
    {
        return fixture.getFileSystemAbstraction();
    }

    protected final File file( String pathname ) throws IOException
    {
        return fixture.file( pathname );
    }

    protected void ensureExists( File file ) throws IOException
    {
        fs.mkdirs( file.getParentFile() );
        fs.create( file ).close();
    }

    protected File existingFile( String name ) throws IOException
    {
        File file = file( name );
        ensureExists( file );
        return file;
    }

    protected void ensureDirectoryExists( File dir )
    {
        fs.mkdir( dir );
    }

    protected File existingDirectory( String name ) throws IOException
    {
        File dir = file( name );
        ensureDirectoryExists( dir );
        return dir;
    }

    /**
     * Verifies the records on the current page of the cursor.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected( PageCursor cursor ) throws IOException
    {
        ByteBuffer expectedPageContents = ByteBuffer.allocate( filePageSize );
        ByteBuffer actualPageContents = ByteBuffer.allocate( filePageSize );
        byte[] record = new byte[recordSize];
        long pageId = cursor.getCurrentPageId();
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (pageId * recordsPerFilePage) + i;
            expectedPageContents.position( recordSize * i );
            ByteBuffer slice = expectedPageContents.slice();
            slice.limit( recordSize );
            generateRecordForId( recordId, slice );
            do
            {
                cursor.setOffset( recordSize * i );
                cursor.getBytes( record );
            }
            while ( cursor.shouldRetry() );
            actualPageContents.position( recordSize * i );
            actualPageContents.put( record );
        }
        assertRecords( pageId, actualPageContents, expectedPageContents );
    }

    /**
     * Verifies the records in the current buffer assuming the given page id.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected( long pageId, int offset, ByteBuffer actualPageContents ) throws IOException
    {
        ByteBuffer expectedPageContents = ByteBuffer.allocate( filePageSize );
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (pageId * recordsPerFilePage) + i;
            expectedPageContents.position( recordSize * i );
            ByteBuffer slice = expectedPageContents.slice();
            slice.limit( recordSize );
            generateRecordForId( recordId, slice );
        }
        int len = actualPageContents.limit() - actualPageContents.position();
        byte[] actual = new byte[len];
        byte[] expected = new byte[len];
        actualPageContents.get( actual );
        expectedPageContents.position( offset );
        expectedPageContents.get( expected );
        assertRecords( pageId, actual, expected );
    }

    protected void assertRecords( long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents )
    {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
        assertRecords( pageId, actualBytes, expectedBytes );
    }

    protected void assertRecords( long pageId, byte[] actualBytes, byte[] expectedBytes )
    {
        int estimatedPageId = estimateId( actualBytes );
        assertThat(
                "Page id: " + pageId + " " +
                "(based on record data, it should have been " +
                estimatedPageId + ", a difference of " +
                Math.abs( pageId - estimatedPageId ) + ")",
                actualBytes,
                byteArray( expectedBytes ) );
    }

    protected int estimateId( byte[] record )
    {
        return ByteBuffer.wrap( record ).getInt() - 1;
    }

    /**
     * Fill the page bound by the cursor with records that can be verified with
     * {@link #verifyRecordsMatchExpected(PageCursor)} or {@link #verifyRecordsInFile(java.io.File, int)}.
     */
    protected void writeRecords( PageCursor cursor )
    {
        cursor.setOffset( 0 );
        for ( int i = 0; i < recordsPerFilePage; i++ )
        {
            long recordId = (cursor.getCurrentPageId() * recordsPerFilePage) + i;
            generateRecordForId( recordId, bufA );
            cursor.putBytes( bufA.array() );
        }
    }

    protected void generateFileWithRecords(
            File file,
            int recordCount,
            int recordSize ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, OpenMode.READ_WRITE ) )
        {
            generateFileWithRecords( channel, recordCount, recordSize );
        }
    }

    protected void generateFileWithRecords( WritableByteChannel channel, int recordCount, int recordSize )
            throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            int rem = buf.remaining();
            do
            {
                rem -= channel.write( buf );
            }
            while ( rem > 0 );
        }
    }

    protected static void generateRecordForId( long id, ByteBuffer buf )
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

    protected void verifyRecordsInFile( File file, int recordCount ) throws IOException
    {
        try ( StoreChannel channel = fs.open( file, OpenMode.READ ) )
        {
            verifyRecordsInFile( channel, recordCount );
        }
    }

    protected void verifyRecordsInFile( ReadableByteChannel channel, int recordCount ) throws IOException
    {
        ByteBuffer buf = ByteBuffer.allocate( recordSize );
        ByteBuffer observation = ByteBuffer.allocate( recordSize );
        for ( int i = 0; i < recordCount; i++ )
        {
            generateRecordForId( i, buf );
            observation.position( 0 );
            channel.read( observation );
            assertRecords( i, observation, buf );
        }
    }

    protected Runnable $close( final PagedFile file )
    {
        return () ->
        {
            try
            {
                file.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError( e );
            }
        };
    }

    public abstract static class Fixture<T extends PageCache>
    {
        public abstract T createPageCache( PageSwapperFactory swapperFactory, int maxPages,
                                           PageCacheTracer tracer, PageCursorTracerSupplier cursorTracerSupplier,
                                           VersionContextSupplier contextSupplier );

        public abstract void tearDownPageCache( T pageCache );

        private Supplier<FileSystemAbstraction> fileSystemAbstractionSupplier = EphemeralFileSystemAbstraction::new;
        private Function<String,File> fileConstructor = File::new;

        public final FileSystemAbstraction getFileSystemAbstraction()
        {
            return fileSystemAbstractionSupplier.get();
        }

        public final Fixture<T> withFileSystemAbstraction(
                Supplier<FileSystemAbstraction> fileSystemAbstractionSupplier )
        {
            this.fileSystemAbstractionSupplier = fileSystemAbstractionSupplier;
            return this;
        }

        public final File file( String pathname ) throws IOException
        {
            return fileConstructor.apply( pathname ).getCanonicalFile();
        }

        public final Fixture<T> withFileConstructor( Function<String,File> fileConstructor )
        {
            this.fileConstructor = fileConstructor;
            return this;
        }
    }
}
