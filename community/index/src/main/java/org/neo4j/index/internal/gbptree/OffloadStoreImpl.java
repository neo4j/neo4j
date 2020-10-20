/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.checkOutOfBounds;

/**
 * Offload page layout: [HEADER 9B| KEYSIZE 4B| VALUESIZE 4B | KEY | VALUE]
 * [HEADER]: [TYPE 1B | RESERVED SPACE 8B]
 * Key and value size are simple integers
 * Key and value layout is decided by layout.
 */
public class OffloadStoreImpl<KEY,VALUE> implements OffloadStore<KEY,VALUE>
{
    @VisibleForTesting
    static final int SIZE_HEADER = Byte.BYTES + Long.BYTES;
    private static final int SIZE_KEY_SIZE = Integer.BYTES;
    private static final int SIZE_VALUE_SIZE = Integer.BYTES;
    private final Layout<KEY,VALUE> layout;
    private final IdProvider idProvider;
    private final OffloadPageCursorFactory pcFactory;
    private final OffloadIdValidator offloadIdValidator;
    private final int maxEntrySize;

    OffloadStoreImpl( Layout<KEY,VALUE> layout, IdProvider idProvider, OffloadPageCursorFactory pcFactory, OffloadIdValidator offloadIdValidator, int pageSize )
    {
        this.layout = layout;
        this.idProvider = idProvider;
        this.pcFactory = pcFactory;
        this.offloadIdValidator = offloadIdValidator;
        this.maxEntrySize = keyValueSizeCapFromPageSize( pageSize );
    }

    @Override
    public int maxEntrySize()
    {
        return maxEntrySize;
    }

    @Override
    public void readKey( long offloadId, KEY into, PageCursorTracer cursorTracer ) throws IOException
    {
        validateOffloadId( offloadId );

        try ( PageCursor cursor = pcFactory.create( offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                if ( !readHeader( cursor ) )
                {
                    continue;
                }
                cursor.setOffset( SIZE_HEADER );
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 || valueSize < 0 )
                {
                    readUnreliableKeyValueSize( cursor, keySize, valueSize );
                    continue;
                }
                layout.readKey( cursor, into, keySize );
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public void readKeyValue( long offloadId, KEY key, VALUE value, PageCursorTracer cursorTracer ) throws IOException
    {
        validateOffloadId( offloadId );

        try ( PageCursor cursor = pcFactory.create( offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                if ( !readHeader( cursor ) )
                {
                    continue;
                }
                cursor.setOffset( SIZE_HEADER );
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 || valueSize < 0 )
                {
                    readUnreliableKeyValueSize( cursor, keySize, valueSize );
                    continue;
                }
                layout.readKey( cursor, key, keySize );
                layout.readValue( cursor, value, valueSize );
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public void readValue( long offloadId, VALUE into, PageCursorTracer cursorTracer ) throws IOException
    {
        validateOffloadId( offloadId );

        try ( PageCursor cursor = pcFactory.create( offloadId, PagedFile.PF_SHARED_READ_LOCK, cursorTracer ) )
        {
            do
            {
                placeCursorAtOffloadId( cursor, offloadId );

                if ( !readHeader( cursor ) )
                {
                    continue;
                }
                cursor.setOffset( SIZE_HEADER );
                int keySize = cursor.getInt();
                int valueSize = cursor.getInt();
                if ( keyValueSizeTooLarge( keySize, valueSize ) || keySize < 0 || valueSize < 0 )
                {
                    readUnreliableKeyValueSize( cursor, keySize, valueSize );
                    continue;
                }
                cursor.setOffset( cursor.getOffset() + keySize );
                layout.readValue( cursor, into, valueSize );
            }
            while ( cursor.shouldRetry() );
            checkOutOfBounds( cursor );
            cursor.checkAndClearCursorException();
        }
    }

    @Override
    public long writeKey( KEY key, long stableGeneration, long unstableGeneration, PageCursorTracer cursorTracer ) throws IOException
    {
        int keySize = layout.keySize( key );
        long newId = acquireNewId( stableGeneration, unstableGeneration, cursorTracer );
        try ( PageCursor cursor = pcFactory.create( newId, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer ) )
        {
            placeCursorAtOffloadId( cursor, newId );

            writeHeader( cursor );
            cursor.setOffset( SIZE_HEADER );
            putKeyValueSize( cursor, keySize, 0 );
            layout.writeKey( cursor, key );
            return newId;
        }
    }

    @Override
    public long writeKeyValue( KEY key, VALUE value, long stableGeneration, long unstableGeneration, PageCursorTracer cursorTracer ) throws IOException
    {
        int keySize = layout.keySize( key );
        int valueSize = layout.valueSize( value );
        long newId = acquireNewId( stableGeneration, unstableGeneration, cursorTracer );
        try ( PageCursor cursor = pcFactory.create( newId, PagedFile.PF_SHARED_WRITE_LOCK, cursorTracer ) )
        {
            placeCursorAtOffloadId( cursor, newId );

            writeHeader( cursor );
            cursor.setOffset( SIZE_HEADER );
            putKeyValueSize( cursor, keySize, valueSize );
            layout.writeKey( cursor, key );
            layout.writeValue( cursor, value );
            return newId;
        }
    }

    @Override
    public void free( long offloadId, long stableGeneration, long unstableGeneration, PageCursorTracer cursorTracer ) throws IOException
    {
        idProvider.releaseId( stableGeneration, unstableGeneration, offloadId, cursorTracer );
    }

    @VisibleForTesting
    static int keyValueSizeCapFromPageSize( int pageSize )
    {
        return pageSize - SIZE_HEADER - SIZE_KEY_SIZE - SIZE_VALUE_SIZE;
    }

    static void writeHeader( PageCursor cursor )
    {
        cursor.putByte( TreeNode.BYTE_POS_NODE_TYPE, TreeNode.NODE_TYPE_OFFLOAD );
    }

    private boolean readHeader( PageCursor cursor )
    {
        byte type = TreeNode.nodeType( cursor );
        if ( type != TreeNode.NODE_TYPE_OFFLOAD )
        {
            cursor.setCursorException( format( "Tried to read from offload store but page is not an offload page. Expected %d but was %d",
                    TreeNode.NODE_TYPE_OFFLOAD, type ) );
            return false;
        }
        return true;
    }

    @VisibleForTesting
    static void putKeyValueSize( PageCursor cursor, int keySize, int valueSize )
    {
        cursor.putInt( keySize );
        cursor.putInt( valueSize );
    }

    private long acquireNewId( long stableGeneration, long unstableGeneration, PageCursorTracer cursorTracer ) throws IOException
    {
        return idProvider.acquireNewId( stableGeneration, unstableGeneration, cursorTracer );
    }

    private static void placeCursorAtOffloadId( PageCursor cursor, long offloadId ) throws IOException
    {
        PageCursorUtil.goTo( cursor, "offload page", offloadId );
    }

    private boolean keyValueSizeTooLarge( int keySize, int valueSize )
    {
        return keySize > maxEntrySize || valueSize > maxEntrySize || (keySize + valueSize) > maxEntrySize;
    }

    private static void readUnreliableKeyValueSize( PageCursor cursor, int keySize, int valueSize )
    {
        cursor.setCursorException( format( "Read unreliable key, id=%d, keySize=%d, valueSize=%d", cursor.getCurrentPageId(), keySize, valueSize ) );
    }

    private void validateOffloadId( long offloadId ) throws IOException
    {
        if ( !offloadIdValidator.valid( offloadId ) )
        {
            throw new IOException( String.format( "Offload id %d is outside of valid range, ", offloadId ) );
        }
    }
}
