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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Factory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;
import org.neo4j.unsafe.impl.batchimport.store.io.Ring;

import static java.lang.Math.min;

/**
 * {@link PageCache} that is optimized for single threaded batched access.
 * There's only one cursor that moves with the supposedly sequential access when page boundaries are
 * crossed. As part of moving the window the data in the page (residing in a {@link ByteBuffer}) is written
 * to the channel.
 */
public class BatchingPageCache implements PageCache
{
    public interface WriterFactory
    {
        Writer create( File file, StoreChannel channel, Monitor monitor );
    }

    /**
     * Receives requests to write data to the underlying channel.
     */
    public interface Writer
    {
        void write( ByteBuffer byteBuffer, long position, Ring<ByteBuffer> ring ) throws IOException;
    }

    public static final WriterFactory SYNCHRONOUS = new WriterFactory()
    {
        @Override
        public Writer create( final File file, final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                @Override
                public void write( ByteBuffer data, long position, Ring<ByteBuffer> ring ) throws IOException
                {
                    try
                    {
                        int written = channel.write( data, position );
                        monitor.dataWritten( written );
                    }
                    finally
                    {
                        ring.free( data );
                    }
                }
            };
        }
    };

    public static enum Mode
    {
        APPEND_ONLY
                {
                    @Override
                    boolean canReadFrom( long pageId )
                    {
                        return pageId == 0;
                    }
                },
        UPDATE
                {
                    @Override
                    boolean canReadFrom( long pageId )
                    {
                        return true;
                    }
                };

        abstract boolean canReadFrom( long pageId );
    }

    private final int pageSize;
    private final FileSystemAbstraction fs;
    private final Map<File, BatchingPagedFile> pagedFiles = new HashMap<>();
    private final WriterFactory writerFactory;
    private final Monitor monitor;
    private final Mode mode;

    private static final ByteBuffer ZEROS = ByteBuffer.allocateDirect( 1024 * 4 );

    public BatchingPageCache( FileSystemAbstraction fs, int pageSize, WriterFactory writerFactory,
                              Monitor monitor, Mode mode )
    {
        this.fs = fs;
        this.pageSize = pageSize;
        this.writerFactory = writerFactory;
        this.monitor = monitor;
        this.mode = mode;
    }

    @Override
    public PagedFile map( File file, int pageSize ) throws IOException
    {
        StoreChannel channel = fs.open( file, "rw" );
        BatchingPagedFile pageFile = new BatchingPagedFile( channel,
                writerFactory.create( file, channel, monitor ), pageSize, mode );
        pagedFiles.put( file, pageFile );
        return pageFile;
    }

    @Override
    public void unmap( File file ) throws IOException
    {
        BatchingPagedFile pageFile = pagedFiles.remove( file );
        if ( pageFile == null )
        {
            throw new IllegalArgumentException( file.toString() );
        }
        pageFile.close();
    }

    @Override
    public void flush() throws IOException
    {   // no need to do anything here
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

    static class BatchingPagedFile implements PagedFile
    {
        private final BatchingPageCursor singleCursor;
        private final StoreChannel channel;
        private final int pageSize;

        public BatchingPagedFile( StoreChannel channel, Writer writer, int pageSize, Mode mode ) throws IOException
        {
            this.channel = channel;
            this.pageSize = pageSize;
            this.singleCursor = new BatchingPageCursor( channel, writer, pageSize, mode );
        }

        @Override
        public PageCursor io( long pageId, int pf_flags ) throws IOException
        {
            singleCursor.ensurePagePlacedOver( pageId );
            // Do this so that the first call to next() will have the cursor "placed" there
            // and consecutive calls move the cursor forwards.
            singleCursor.pinned = false;
            return singleCursor;
        }

        @Override
        public int pageSize()
        {
            return pageSize;
        }

        @Override
        public void close() throws IOException
        {
            flush();
            singleCursor.close();
            channel.close();
        }

        @Override
        public int numberOfCachedPages()
        {
            return 1;
        }

        @Override
        public void flush() throws IOException
        {
            singleCursor.flush();
            force();
        }

        @Override
        public void force() throws IOException
        {
            channel.force( true );
        }

        @Override
        public long getLastPageId() throws IOException
        {
            return singleCursor.highestKnownPageId();
        }
    }

    static class BatchingPageCursor implements PageCursor
    {
        private final Ring<ByteBuffer> bufferRing;
        private ByteBuffer currentBuffer;
        private final StoreChannel channel;
        private final Writer writer;
        private long currentPageId = -1;
        private final Mode mode;
        private final int pageSize;
        private boolean pinned;
        private long highestKnownPageId;

        BatchingPageCursor( StoreChannel channel, Writer writer, final int pageSize, Mode mode ) throws IOException
        {
            this.channel = channel;
            this.writer = writer;
            this.pageSize = pageSize;
            this.mode = mode;
            this.bufferRing = new Ring<>( 2, new Factory<ByteBuffer>()
            {
                @Override
                public ByteBuffer newInstance()
                {
                    return ByteBuffer.allocateDirect( pageSize );
                }
            } );
            this.currentBuffer = bufferRing.next();
            this.highestKnownPageId = channel.size() / pageSize;
        }

        @Override
        public byte getByte()
        {
            return currentBuffer.get();
        }

        @Override
        public void putByte( byte value )
        {
            currentBuffer.put( value );
        }

        @Override
        public long getLong()
        {
            return currentBuffer.getLong();
        }

        @Override
        public void putLong( long value )
        {
            currentBuffer.putLong( value );
        }

        @Override
        public int getInt()
        {
            return currentBuffer.getInt();
        }

        @Override
        public void putInt( int value )
        {
            currentBuffer.putInt( value );
        }

        @Override
        public long getUnsignedInt()
        {
            return getInt() & 0xFFFFFFFFL;
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
        }

        @Override
        public short getShort()
        {
            return currentBuffer.getShort();
        }

        @Override
        public void putShort( short value )
        {
            currentBuffer.putShort( value );
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
        public void rewind() throws IOException
        {
            throw new UnsupportedOperationException(
                    "Unsupported in this batching page cache, since it's all about strictly sequential access" );
        }

        @Override
        public boolean next() throws IOException
        {
            return next( currentPageId + 1 );
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
        public boolean retry()
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

            // If we're not in append-only mode, i.e. if we're in update mode
            // OR if this is the first window index we read the contents.
            // The reason for reading the first windows is that in order to play nicely with
            // NeoStore and loading the store sometimes header information needs to be read,
            // even if we're in append-only mode
            if ( mode.canReadFrom( pageId ) )
            {
                readFromChannelIntoBuffer( pageId );
            }
            else
            {
                zeroBuffer( currentBuffer );
            }
            // buffer position after we placed the window is irrelevant since every future access
            // will set offset explicitly before use.
            currentPageId = pageId;
            highestKnownPageId = Math.max( highestKnownPageId, pageId );
            prepared( currentBuffer );
        }

        private void readFromChannelIntoBuffer( long pageId ) throws IOException
        {
            channel.read( prepared( currentBuffer ), pageId * pageSize );
        }

        private void flush() throws IOException
        {
            if ( currentPageId == -1 )
            {
                return;
            }

            writer.write( prepared( currentBuffer ), currentPageId * pageSize, bufferRing );
            currentBuffer = bufferRing.next();
            currentBuffer.clear();
            currentPageId = -1;
        }

        private ByteBuffer prepared( ByteBuffer buffer )
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
