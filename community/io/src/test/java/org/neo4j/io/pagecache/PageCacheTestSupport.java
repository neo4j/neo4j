/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.RepeatRule;

import static org.junit.Assert.assertThat;
import static org.neo4j.test.ByteArrayMatcher.byteArray;

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
    protected int pageCachePageSize = 32;
    protected int recordsPerFilePage = pageCachePageSize / recordSize;
    protected int recordCount = 25 * maxPages * recordsPerFilePage;
    protected int filePageSize = recordsPerFilePage * recordSize;
    protected ByteBuffer bufA = ByteBuffer.allocate( recordSize );
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

    protected final T createPageCache(
            PageSwapperFactory swapperFactory,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer )
    {
        return fixture.createPageCache( swapperFactory, maxPages, pageSize, tracer );
    }

    protected T createPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer )
    {
        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory();
        swapperFactory.setFileSystemAbstraction( fs );
        return createPageCache( swapperFactory, maxPages, pageSize, tracer );
    }

    protected final T getPageCache(
            FileSystemAbstraction fs,
            int maxPages,
            int pageSize,
            PageCacheTracer tracer ) throws IOException
    {
        if ( pageCache != null )
        {
            tearDownPageCache( pageCache );
        }
        pageCache = createPageCache( fs, maxPages, pageSize, tracer );
        return pageCache;
    }

    protected void configureStandardPageCache() throws IOException
    {
        getPageCache( fs, maxPages, pageCachePageSize, PageCacheTracer.NULL );
    }

    protected final void tearDownPageCache( T pageCache ) throws IOException
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

    protected void ensureDirectoryExists( File dir ) throws IOException
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
            generateRecordForId( recordId, expectedPageContents.slice() );
            do
            {
                cursor.setOffset( recordSize * i );
                cursor.getBytes( record );
            }
            while ( cursor.shouldRetry() );
            actualPageContents.position( recordSize * i );
            actualPageContents.put( record );
        }
        assertRecord( pageId, actualPageContents, expectedPageContents );
    }

    protected void assertRecord( long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents )
    {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
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
        try ( StoreChannel channel = fs.open( file, "rw" ) )
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
        try ( StoreChannel channel = fs.open( file, "r" ) )
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
            assertRecord( i, observation, buf );
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
        public abstract T createPageCache(
                PageSwapperFactory swapperFactory,
                int maxPages,
                int pageSize,
                PageCacheTracer tracer );

        public abstract void tearDownPageCache( T pageCache ) throws IOException;

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
