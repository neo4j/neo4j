/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store;

import sun.misc.Cleaner;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.collection.pool.Pool;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.util.SimplePool;
import org.neo4j.unsafe.impl.batchimport.Parallelizable;
import org.neo4j.unsafe.impl.batchimport.WriterFactories;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static java.lang.Math.max;
import static java.lang.Math.min;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

/**
* {@link PageCache} that is optimized for single threaded batched access.
* There's only one cursor that moves with the supposedly sequential access when page boundaries are
* crossed. As part of moving the window the data in the page (residing in a {@link ByteBuffer}) is written
* to the channel.
*/
@SuppressWarnings( "restriction" )
public class BatchingPageCache implements PageCache
{
    public interface WriterFactory extends Parallelizable
    {
        Writer create( StoreChannel channel, Monitor monitor );

        void awaitEverythingWritten();

        void shutdown();
    }

    /**
     * Receives requests to write data to the underlying channel.
     */
    public interface Writer
    {
        void write( ByteBuffer byteBuffer, long position, Pool<ByteBuffer> poolToReleaseBufferIn ) throws IOException;
    }

    public static final WriterFactory SYNCHRONOUS = new WriterFactories.SingleThreadedWriterFactory()
    {
        @Override
        public Writer create( final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                @Override
                public void write( ByteBuffer data, long position, Pool<ByteBuffer> poolToReleaseBufferIn ) throws IOException
                {
                    try
                    {
                        int written = channel.write( data, position );
                        monitor.dataWritten( written );
                    }
                    finally
                    {
                        poolToReleaseBufferIn.release( data );
                    }
                }
            };
        }

        @Override
        public void awaitEverythingWritten()
        {   // no-op
        }

        @Override
        public void shutdown()
        {   // no-op
        }

        @Override
        public String toString()
        {
            return "SYNCHRONOUS";
        }
    };

    private final int pageSize;
    private final int bigFileMultiplier;
    private final FileSystemAbstraction fs;
    private final Map<File, BatchingPagedFile> pagedFiles = new HashMap<>();
    private final WriterFactory writerFactory;
    private final Monitor monitor;

    public BatchingPageCache( FileSystemAbstraction fs, int pageSize, int bigFileMultiplier,
            WriterFactory writerFactory, Monitor monitor )
    {
        this.fs = fs;
        this.pageSize = pageSize;
        this.bigFileMultiplier = bigFileMultiplier;
        this.writerFactory = writerFactory;
        this.monitor = monitor;
    }

    @Override
    public PagedFile map( final File file, int pageSize ) throws IOException
    {
        StoreChannel channel = fs.open( file, "rw" );
        // This is a hack necessary to make sure that we write to disk immediately the changes to the
        // counts store since we circumvent the page cache to read the counts
        Writer writer = file.getName().contains( StoreFactory.COUNTS_STORE )
                ? SYNCHRONOUS.create( channel, monitor )
                : writerFactory.create( channel, monitor );
        BatchingPagedFile pageFile = new BatchingPagedFile(channel, writer,
                individualizedPageSize( file, pageSize ), new Closeable()
                {
                    @Override
                    public void close() throws IOException
                    {
                        BatchingPagedFile pageFile = pagedFiles.remove( file );
                        if ( pageFile == null )
                        {
                            throw new IllegalArgumentException( file.toString() );
                        }
                    }
                } );
        pagedFiles.put( file, pageFile );
        return pageFile;
    }

    private int individualizedPageSize( File file, int pageSize )
    {
        // There's a problem, at least on Windows 7, where the OS would somehow
        // keep the entire file, or very large portions of it in memory when reading
        // the relationship store backwards.
        // This would be a performance problem where releasing of this memory would
        // come first when the import was done, or all available RAM was used at
        // which point the OS would start releasing some parts of the cached
        // relationship file. Although at this point the JVM would observe
        // significant stalls and slowdowns.
        // The current solution is to, when reading a file backwards, read it in
        // much bigger chunks. When doing so the OS doesn't seem to cache the file
        // like described above.
        return file.getName().endsWith( StoreFactory.RELATIONSHIP_STORE_NAME )
                ? maxPercentageOfHeapThough( pageSize * bigFileMultiplier, 10 ) : pageSize;
    }

    /**
     * This method is introduced so that this page cache can function reasonably well even on small heaps,
     * so that the relationship store buffer doesn't eat up most available off-heap (controlled by Bits class)
     * memory, which is a percentage of total heap or something.
     */
    private int maxPercentageOfHeapThough( int size, float maxPercentageOfHeap )
    {
        return min( size, (int)(Runtime.getRuntime().maxMemory() * (maxPercentageOfHeap/100f)) );
    }

    @Override
    public void flushAndForce() throws IOException
    {   // no need to do anything here
        for ( PagedFile file : pagedFiles.values() )
        {
            file.flushAndForce();
        }
    }

    @Override
    public void close() throws IOException
    {
        for ( BatchingPagedFile pagedFile : pagedFiles.values() )
        {
            pagedFile.close();
        }
        pagedFiles.clear();
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    @Override
    public int maxCachedPages()
    {
        return 1;
    }

    private static final int READ_WRITE_PF_FLAGS = PF_SHARED_LOCK | PF_EXCLUSIVE_LOCK;

    private static class BatchingPagedFile implements PagedFile
    {
        private final BatchingPageCursor[] cursors = new BatchingPageCursor[READ_WRITE_PF_FLAGS];
        private final StoreChannel channel;
        private final int pageSize;
        private final Closeable resource;

        public BatchingPagedFile( StoreChannel channel, Writer writer, int pageSize, Closeable resource )
                throws IOException
        {
            this.channel = channel;
            this.pageSize = pageSize;
            this.resource = resource;
            this.cursors[PF_SHARED_LOCK] = new ReadCursor( channel, writer, pageSize );
            this.cursors[PF_EXCLUSIVE_LOCK] = new WriteCursor( channel, writer, pageSize );
        }

        @Override
        public PageCursor io( long pageId, int pf_flags ) throws IOException
        {
            BatchingPageCursor cursor = cursor( pf_flags );
            cursor.ensurePagePlacedOver( pageId );
            // Do this so that the first call to next() will have the cursor "placed" there
            // and consecutive calls move the cursor forwards.
            cursor.pinned = false;
            return cursor;
        }

        private BatchingPageCursor cursor( int pf_flags )
        {
            assert (pf_flags & READ_WRITE_PF_FLAGS) != 0 && (pf_flags & READ_WRITE_PF_FLAGS) != READ_WRITE_PF_FLAGS :
                "Unexpected set pf flags " + pf_flags;
            return cursors[pf_flags & READ_WRITE_PF_FLAGS];
        }

        @Override
        public int pageSize()
        {
            return pageSize;
        }

        @Override
        public void close() throws IOException
        {
            resource.close();
            closeFile();
            cursors[PF_SHARED_LOCK].free();
            cursors[PF_EXCLUSIVE_LOCK].free();
        }

        public void closeFile() throws IOException
        {
            flushAndForce();
            channel.close();
        }

        @Override
        public void flushAndForce() throws IOException
        {
            cursors[PF_SHARED_LOCK].flush();
            cursors[PF_EXCLUSIVE_LOCK].flush();
        }

        @Override
        public void force() throws IOException
        {   // no-op
        }

        @Override
        public long getLastPageId() throws IOException
        {
            return max( cursors[PF_SHARED_LOCK].highestKnownPageId(), cursors[PF_EXCLUSIVE_LOCK].highestKnownPageId );
        }
    }

    private static abstract class BatchingPageCursor implements PageCursor
    {
        protected ByteBuffer currentBuffer;
        private final ByteBuffer[] buffers;
        protected final SimplePool<ByteBuffer> bufferPool;
        private final StoreChannel channel;
        protected final Writer writer;
        protected long currentPageId = -1;
        protected final int pageSize;
        private boolean pinned;
        private long highestKnownPageId;
        protected boolean changed;

        BatchingPageCursor( StoreChannel channel, Writer writer, final int pageSize, int bufferCount )
                throws IOException
        {
            this.channel = channel;
            this.writer = writer;
            this.pageSize = pageSize;
            this.buffers = new ByteBuffer[bufferCount];
            for ( int i = 0; i < buffers.length; i++ )
            {
                try
                {
                    buffers[i] = ByteBuffer.allocateDirect( pageSize );
                }
                catch ( OutOfMemoryError e )
                {
                    buffers[i] = ByteBuffer.allocate( pageSize );
                }
            }
            this.bufferPool = new SimplePool<>( buffers );
            this.currentBuffer = bufferPool.acquire();
            this.highestKnownPageId = channel.size() / pageSize;
        }

        private void free()
        {
            for ( ByteBuffer buffer : buffers )
            {
                optimisticallyAndPreemtivelyFree( buffer );
            }
        }

        private void optimisticallyAndPreemtivelyFree( ByteBuffer byteBuffer )
        {
            if ( byteBuffer.isDirect() )
            {
                try
                {
                    Method method = byteBuffer.getClass().getMethod( "cleaner" );
                    method.setAccessible( true );
                    Cleaner cleaner = (Cleaner) method.invoke( byteBuffer );
                    cleaner.clean();
                }
                catch ( Exception e )
                {   // It's alright, we tried
                }
            }
        }

        @Override
        public byte getByte()
        {
            return currentBuffer.get();
        }

        @Override
        public byte getByte(int offset)
        {
            return currentBuffer.get(offset);
        }

        @Override
        public void putByte( byte value )
        {
            currentBuffer.put( value );
            changed = true;
        }

        @Override
        public void putByte( int offset, byte value )
        {
            currentBuffer.put( offset, value );
        }

        @Override
        public long getLong()
        {
            return currentBuffer.getLong();
        }

        @Override
        public long getLong(int offset)
        {
            return currentBuffer.getLong(offset);
        }

        @Override
        public void putLong( long value )
        {
            currentBuffer.putLong( value );
            changed = true;
        }

        @Override
        public void putLong( int offset, long value )
        {
            currentBuffer.putLong( offset, value );
        }

        @Override
        public int getInt()
        {
            return currentBuffer.getInt();
        }

        @Override
        public int getInt(int offset)
        {
            return currentBuffer.getInt(offset);
        }

        @Override
        public void putInt( int value )
        {
            currentBuffer.putInt( value );
            changed = true;
        }

        @Override
        public void putInt( int offset, int value )
        {
            currentBuffer.putInt( offset, value );
        }

        @Override
        public long getUnsignedInt()
        {
            return getInt() & 0xFFFFFFFFL;
        }

        @Override
        public long getUnsignedInt(int offset)
        {
            return getInt( offset ) & 0xFFFFFFFFL;
        }

        @Override
        public void getBytes( byte[] data )
        {
            currentBuffer.get( data );
        }

        @Override
        public void putBytes( byte[] data )
        {
            currentBuffer.put( data );
            changed = true;
        }

        @Override
        public short getShort()
        {
            return currentBuffer.getShort();
        }

        @Override
        public short getShort(int offset)
        {
            return currentBuffer.getShort(offset);
        }

        @Override
        public void putShort( short value )
        {
            currentBuffer.putShort( value );
            changed = true;
        }

        @Override
        public void putShort( int offset, short value )
        {
            currentBuffer.putShort( offset, value );
        }

        @Override
        public void setOffset( int offset )
        {
            currentBuffer.position( offset );
        }

        @Override
        public int getOffset()
        {
            return currentBuffer.position();
        }

        @Override
        public long getCurrentPageId()
        {
            return currentPageId;
        }

        @Override
        public int getCurrentPageSize()
        {
            return currentPageId == -1? UNBOUND_PAGE_SIZE : pageSize;
        }

        @Override
        public File getCurrentFile()
        {
            // We don't need this where the BatchingPageCache is being used.
            throw new UnsupportedOperationException();
        }

        @Override
        public void rewind() throws IOException
        {
            throw new UnsupportedOperationException(
                    "Unsupported in this batching page cache, since it's all about strictly sequential access" );
        }

        @Override
        public boolean next() throws IOException
        {
            return next( currentPageId+1 );
        }

        @Override
        public boolean next( long pageId ) throws IOException
        {
            if ( !pinned )
            {
                pinned = true;
                return true;
            }

            ensurePagePlacedOver( pageId );
            return true;
        }

        @Override
        public void close()
        {   // no-op
        }

        @Override
        public boolean shouldRetry()
        {
            return false;
        }

        private void ensurePagePlacedOver( long pageId ) throws IOException
        {
            if ( pageId == currentPageId )
            {
                return;
            }

            flush();
            placeBufferAt( currentBuffer, pageId );

            // buffer position after we placed the window is irrelevant since every future access
            // will set offset explicitly before use.
            currentPageId = pageId;
            highestKnownPageId = Math.max( highestKnownPageId, pageId );
            prepared( currentBuffer );
        }

        protected abstract void placeBufferAt( ByteBuffer buffer, long pageId ) throws IOException;

        protected void readFromChannelIntoBuffer( long pageId ) throws IOException
        {
            channel.read( prepared( currentBuffer ), pageId*pageSize );
        }

        private void flush() throws IOException
        {
            if ( currentPageId == -1 )
            {
                return;
            }
            doFlush();
            currentPageId = -1;
        }

        protected abstract void doFlush() throws IOException;

        protected ByteBuffer prepared( ByteBuffer buffer )
        {
            buffer.flip();
            buffer.limit( pageSize ); // always write the full page
            return buffer;
        }

        public long highestKnownPageId()
        {
            return highestKnownPageId;
        }
    }

    private static class ReadCursor extends BatchingPageCursor
    {
        ReadCursor( StoreChannel channel, Writer writer, int pageSize ) throws IOException
        {
            super( channel, writer, pageSize, 1 );
        }

        @Override
        protected void placeBufferAt( ByteBuffer buffer, long pageId ) throws IOException
        {
            readFromChannelIntoBuffer( pageId );
        }

        @Override
        protected void doFlush() throws IOException
        {   // No flushing
            assert !changed;
        }
    }

    private static class WriteCursor extends BatchingPageCursor
    {
        WriteCursor( StoreChannel channel, Writer writer, int pageSize ) throws IOException
        {
            super( channel, writer, pageSize, 2 /*double buffering to support asynchronous writes*/ );
        }

        @Override
        protected void placeBufferAt( ByteBuffer buffer, long pageId ) throws IOException
        {
            if ( pageId == 0 )
            {
                readFromChannelIntoBuffer( pageId );
            }
            else
            {
                zeroBuffer( buffer );
            }
        }

        @Override
        protected void doFlush() throws IOException
        {
            if ( changed )
            {
                writer.write( prepared( currentBuffer ), currentPageId * pageSize, bufferPool );
                currentBuffer = bufferPool.acquire();
                changed = false;
            }
        }
    }

    private static final ByteBuffer ZEROS = ByteBuffer.allocateDirect( 1024*4 );

    private static void zeroBuffer( ByteBuffer buffer )
    {
        // Duplicate for thread safety
        ByteBuffer zeros = ZEROS.duplicate();
        buffer.clear();
        while ( buffer.hasRemaining() )
        {
            int chunkSize = min( buffer.remaining(), zeros.capacity() );
            zeros.clear();
            zeros.limit( chunkSize );
            buffer.put( zeros );
        }
    }
}
