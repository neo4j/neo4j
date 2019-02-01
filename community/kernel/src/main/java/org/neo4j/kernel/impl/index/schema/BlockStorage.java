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
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.util.Preconditions;

import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.getOverhead;
import static org.neo4j.index.internal.gbptree.DynamicSizeUtil.putKeyValueSize;

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
    private final int blockSize;
    private final ByteBufferFactory bufferFactory;
    private int currentBufferSize;
    private int mergeIteration;
    private boolean doneAdding;

    BlockStorage( Layout<KEY,VALUE> layout, ByteBufferFactory bufferFactory, FileSystemAbstraction fs, File blockFile, Monitor monitor, int blockSize )
            throws IOException
    {
        this.layout = layout;
        this.fs = fs;
        this.blockFile = blockFile;
        this.monitor = monitor;
        this.blockSize = blockSize;
        this.bufferedEntries = Lists.mutable.empty();
        this.bufferFactory = bufferFactory;
        this.byteBuffer = bufferFactory.newBuffer( blockSize );
        this.comparator = ( e0, e1 ) -> layout.compare( e0.key(), e1.key() );
        this.storeChannel = fs.create( blockFile );
        resetBuffer();
    }

    public void add( KEY key, VALUE value ) throws IOException
    {
        Preconditions.checkState( !doneAdding, "Cannot add more after done adding" );

        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        int overhead = getOverhead( keySize, valueSize );
        int entrySize = keySize + valueSize + overhead;

        if ( currentBufferSize + entrySize > blockSize )
        {
            // append buffer to file and clear buffers
            flushAndResetBuffer();
        }

        bufferedEntries.add( new BlockEntry<>( key, value ) );
        currentBufferSize += entrySize;
        monitor.entryAdded( entrySize );
    }

    public void doneAdding() throws IOException
    {
        if ( !bufferedEntries.isEmpty() )
        {
            flushAndResetBuffer();
        }
        doneAdding = true;
    }

    private void resetBuffer()
    {
        byteBuffer.clear();
        bufferedEntries.clear();
        currentBufferSize = BLOCK_HEADER_SIZE;
    }

    private void flushAndResetBuffer() throws IOException
    {
        bufferedEntries.sortThis( comparator );
        ByteArrayPageCursor pageCursor = new ByteArrayPageCursor( byteBuffer );

        System.out.println( "Writing block at " + storeChannel.position() );
        // Header
        pageCursor.putLong( bufferedEntries.size() );

        // Entries
        for ( BlockEntry<KEY,VALUE> entry : bufferedEntries )
        {
            int keySize = layout.keySize( entry.key() );
            int valueSize = layout.valueSize( entry.value() );
            putKeyValueSize( pageCursor, keySize, valueSize );
            layout.writeKey( pageCursor, entry.key() );
            layout.writeValue( pageCursor, entry.value() );
        }

        // TODO solve the BIG padding problem
        // Zero pad
        pageCursor.putBytes( blockSize - currentBufferSize, (byte) 0 );

        // Append to file
        byteBuffer.flip();
        storeChannel.writeAll( byteBuffer );
        monitor.blockFlushed( bufferedEntries.size(), currentBufferSize, storeChannel.position() );
        resetBuffer();
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( storeChannel );
    }

    private long calculateBlockSize()
    {
        return (long) Math.pow( 2, mergeIteration ) * blockSize;
    }

    public BlockStorageReader<KEY,VALUE> reader() throws IOException
    {
        return new BlockStorageReader<>( fs, blockFile, calculateBlockSize(), layout );
    }

    public interface Monitor
    {
        void entryAdded( int entrySize );

        void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush );

        class Adapter implements Monitor
        {
            @Override
            public void entryAdded( int entrySize )
            {   // no-op
            }

            @Override
            public void blockFlushed( long keyCount, int numberOfBytes, long positionAfterFlush )
            {   // no-op
            }
        }

        Monitor NO_MONITOR = new Adapter();
    }
}
