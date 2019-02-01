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
package org.neo4j.kernel.impl.index.schema;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;

import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractKeySize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.extractValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.readKeyValueSize;

class BlockStorage<KEY, VALUE> implements Closeable
{
    static final int BLOCK_HEADER_SIZE = Long.BYTES; // keyCount

    private final Layout<KEY,VALUE> layout;
    private final FileSystemAbstraction fs;
    private final File blockFile;
    private final MutableList<BlockEntry<KEY,VALUE>> bufferedEntries;
    private final ByteBuffer byteBuffer;
    private final Comparator<BlockEntry<KEY,VALUE>> comparator;
    private final StoreChannel storeChannel;
    private final Monitor monitor;
    private final int bufferSize;
    private final ByteBufferFactory bufferFactory;
    private int currentBufferSize;
    private long currentKeyCount;
    private int mergeIteration;

    BlockStorage( Layout<KEY,VALUE> layout, ByteBufferFactory bufferFactory, FileSystemAbstraction fs, File blockFile, Monitor monitor, int bufferSize )
            throws IOException
    {
        this.layout = layout;
        this.fs = fs;
        this.blockFile = blockFile;
        this.monitor = monitor;
        this.bufferSize = bufferSize;
        this.bufferedEntries = Lists.mutable.empty();
        this.bufferFactory = bufferFactory;
        this.byteBuffer = bufferFactory.newBuffer( bufferSize );
        this.comparator = ( e0, e1 ) -> layout.compare( e0.key(), e1.key() );
        this.storeChannel = fs.create( blockFile );
        resetBuffer();
    }

    public void add( KEY key, VALUE value ) throws IOException
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int overhead = getOverhead( keySize, valueSize );
        int entrySize = keySize + valueSize + overhead;

        if ( currentBufferSize + entrySize > bufferSize )
        {
            // append buffer to file and clear buffers
            flushAndResetBuffer();
        }

        bufferedEntries.add( new BlockEntry<>( key, value ) );
        currentBufferSize += entrySize;
        currentKeyCount++;
        monitor.entryAdded( entrySize );
    }

    public void doneAdding() throws IOException
    {
        if ( currentKeyCount > 0 )
        {
            flushAndResetBuffer();
        }
    }

    private void resetBuffer()
    {
        byteBuffer.clear();
        bufferedEntries.clear();
        currentBufferSize = BLOCK_HEADER_SIZE;
        currentKeyCount = 0;
    }

    private void flushAndResetBuffer() throws IOException
    {
        bufferedEntries.sortThis( comparator );
        ByteArrayPageCursor pageCursor = new ByteArrayPageCursor( byteBuffer );

        // Header
        pageCursor.putLong( currentKeyCount );

        // Entries
        for ( BlockEntry<KEY,VALUE> entry : bufferedEntries )
        {
            int keySize = layout.keySize( entry.key() );
            int valueSize = layout.valueSize( entry.value() );
            putKeyValueSize( pageCursor, keySize, valueSize );
            layout.writeKey( pageCursor, entry.key() );
            layout.writeValue( pageCursor, entry.value() );
        }

        // Zero pad
        pageCursor.putBytes( bufferSize - currentBufferSize, (byte) 0 );

        // Append to file
        byteBuffer.flip();
        storeChannel.writeAll( byteBuffer );
        monitor.blockFlushed( currentKeyCount, currentBufferSize, storeChannel.position() );
        resetBuffer();
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( storeChannel );
    }

    private long calculateBlockSize()
    {
        return (long) Math.pow( 2, mergeIteration ) * bufferSize;
    }

    public BlockReader reader() throws IOException
    {
        return new BlockReader( fs.open( blockFile, OpenMode.READ ) );
    }

    public class BlockReader implements Closeable
    {
        private final StoreChannel channel;

        BlockReader( StoreChannel channel )
        {
            this.channel = channel;
        }

        public EntryReader nextBlock() throws IOException
        {
            long position = channel.position();
            if ( position >= channel.size() )
            {
                return null;
            }
            StoreChannel blockChannel = fs.open( blockFile, OpenMode.READ );
            blockChannel.position( position );
            channel.position( position + calculateBlockSize() );
            PageCursor pageCursor = new ReadableChannelPageCursor( new ReadAheadChannel<>( blockChannel ) );
            EntryReader entryReader = new EntryReader( pageCursor );
            return entryReader;
        }

        @Override
        public void close() throws IOException
        {
            channel.close();
        }
    }

    public class EntryReader implements Closeable
    {
        private final long entryCount;
        private final PageCursor pageCursor;
        private final KEY key;
        private final VALUE value;
        private long readEntries;

        EntryReader( PageCursor pageCursor )
        {
            this.pageCursor = pageCursor;
            this.entryCount = pageCursor.getLong();
            this.key = layout.newKey();
            this.value = layout.newValue();
        }

        public boolean next() throws IOException
        {
            if ( readEntries >= entryCount )
            {
                return false;
            }

            long entrySize = readKeyValueSize( pageCursor );
            layout.readKey( pageCursor, key, extractKeySize( entrySize ) );
            layout.readValue( pageCursor, value, extractValueSize( entrySize ) );

            readEntries++;
            return true;
        }

        public long entryCount()
        {
            return entryCount;
        }

        KEY key()
        {
            return key;
        }

        VALUE value()
        {
            return value;
        }

        @Override
        public void close() throws IOException
        {
            pageCursor.close();
        }
    }

    public interface Monitor
    {
        void entryAdded( int entrySize );

        void blockFlushed( long keyCount, int bufferSize, long positionAfterFlush );

        class Adapter implements Monitor
        {
            @Override
            public void entryAdded( int entrySize )
            {   // no-op
            }

            @Override
            public void blockFlushed( long keyCount, int bufferSize, long positionAfterFlush )
            {   // no-op
            }
        }

        Monitor NO_MONITOR = new Adapter();
    }
}
