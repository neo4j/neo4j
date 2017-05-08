/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntPredicate;

import org.neo4j.function.Disposable;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.string.UTF8;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Cursor that provides a view on property blocks of a particular property record.
 * This cursor is reusable and can be re-initialized with
 * {@link #init(IntPredicate, long[], int)} method and cleaned up using {@link #close()}
 * method.
 * <p>
 * During initialization the raw property block {@code long}s are read from
 * the given property record.
 */
class PropertyPayloadCursor implements Disposable
{
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INTERNAL_BYTE_ARRAY_SIZE = 4096;
    private static final int INITIAL_POSITION = -1;

    /**
     * Reusable initial buffer for reading of dynamic records.
     */
    private final ByteBuffer cachedBuffer = ByteBuffer.allocate( INTERNAL_BYTE_ARRAY_SIZE );

    private final DynamicRecord record;
    private final PageCursor stringCursor;
    private final DynamicStringStore stringStore;
    private final PageCursor arrayCursor;
    private final DynamicArrayStore arrayStore;
    private ByteBuffer buffer = cachedBuffer;

    private long[] blocks;
    private int position = INITIAL_POSITION;
    private int numberOfBlocks;
    private IntPredicate propertyKeyIds;
    private boolean exhausted;

    PropertyPayloadCursor( DynamicStringStore stringStore, DynamicArrayStore arrayStore )
    {
        this.record = stringStore.newRecord();
        this.stringStore = stringStore;
        this.stringCursor = stringStore.newPageCursor();
        this.arrayStore = arrayStore;
        this.arrayCursor = arrayStore.newPageCursor();
    }

    void init( IntPredicate propertyKeyIds, long[] blocks, int numberOfBlocks )
    {
        this.blocks = blocks;
        this.propertyKeyIds = propertyKeyIds;
        this.numberOfBlocks = numberOfBlocks;
        position = INITIAL_POSITION;
        buffer = cachedBuffer;
        exhausted = false;
    }

    void close()
    {
        propertyKeyIds = null;
        position = INITIAL_POSITION;
        numberOfBlocks = 0;
        exhausted = false;
        buffer = cachedBuffer;
    }

    boolean next()
    {
        while ( true )
        {
            if ( exhausted )
            {
                return false;
            }

            if ( position == INITIAL_POSITION )
            {
                position = 0;
            }
            else if ( position < numberOfBlocks )
            {
                position += currentBlocksUsed();
            }

            if ( position >= numberOfBlocks || type() == null )
            {
                exhausted = true;
                return false;
            }

            if ( propertyKeyIds.test( propertyKeyId() ) )
            {
                return true;
            }
        }
    }

    PropertyType type()
    {
        long propBlock = currentHeader();
        return PropertyType.getPropertyTypeOrNull( propBlock );
    }

    int propertyKeyId()
    {
        return PropertyBlock.keyIndexId( currentHeader() );
    }

    Object value()
    {
        switch ( type() )
        {
        case BOOL:
            return PropertyBlock.fetchByte( currentHeader() ) == 1;
        case BYTE:
            return PropertyBlock.fetchByte( currentHeader() );
        case SHORT:
            return PropertyBlock.fetchShort( currentHeader() );
        case CHAR:
            return (char) PropertyBlock.fetchShort( currentHeader() );
        case INT:
            return PropertyBlock.fetchInt( currentHeader() );
        case LONG:
        {
            if ( PropertyBlock.valueIsInlined( currentHeader() ) )
            {
                return PropertyBlock.fetchLong( currentHeader() ) >>> 1;
            }
            return blocks[position + 1];
        }
        case FLOAT:
            return Float.intBitsToFloat( PropertyBlock.fetchInt( currentHeader() ) );
        case DOUBLE:
            return Double.longBitsToDouble( blocks[position + 1] );
        case SHORT_STRING:
            return LongerShortString.decode( blocks, position, currentBlocksUsed() );
        case STRING:
        {
            readFromStore( stringStore, stringCursor );
            buffer.flip();
            return UTF8.decode( buffer.array(), 0, buffer.limit() );
        }
        case SHORT_ARRAY:
            return ShortArray.decode( valueAsBits() );
        case ARRAY:
        {
            readFromStore( arrayStore, arrayCursor );
            buffer.flip();
            return readArrayFromBuffer( buffer );
        }
        default:
            throw new IllegalStateException( "No such type:" + type() );
        }
    }

    private long currentHeader()
    {
        return blocks[position];
    }

    private int currentBlocksUsed()
    {
        return type().calculateNumberOfBlocksUsed( currentHeader() );
    }

    private Bits valueAsBits()
    {
        Bits bits = Bits.bits( MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY );
        int blocksUsed = currentBlocksUsed();
        for ( int i = 0; i < blocksUsed; i++ )
        {
            bits.put( blocks[position + i] );
        }
        return bits;
    }

    private void readFromStore( CommonAbstractStore<DynamicRecord,?> store, PageCursor cursor )
    {
        buffer.clear();
        long blockId = PropertyBlock.fetchLong( currentHeader() );
        do
        {
            store.readRecord( blockId, record, FORCE, cursor );
            byte[] data = record.getData();
            if ( buffer.remaining() < data.length )
            {
                buffer.flip();
                ByteBuffer newBuffer = newBiggerBuffer( data.length );
                newBuffer.put( buffer );
                buffer = newBuffer;
            }
            buffer.put( data, 0, data.length );
            blockId = record.getNextBlock();
        }
        while ( !Record.NULL_REFERENCE.is( blockId ) );
    }

    private ByteBuffer newBiggerBuffer( int requiredCapacity )
    {
        int newCapacity;
        do
        {
            newCapacity = buffer.capacity() * 2;
        }
        while ( newCapacity - buffer.limit() < requiredCapacity );

        return ByteBuffer.allocate( newCapacity ).order( ByteOrder.LITTLE_ENDIAN );
    }

    private static Object readArrayFromBuffer( ByteBuffer buffer )
    {
        if ( buffer.limit() <= 0 )
        {
            throw new IllegalStateException( "Given buffer is empty" );
        }

        byte typeId = buffer.get();
        buffer.order( ByteOrder.BIG_ENDIAN );
        try
        {
            if ( typeId == PropertyType.STRING.intValue() )
            {
                int arrayLength = buffer.getInt();
                String[] result = new String[arrayLength];

                for ( int i = 0; i < arrayLength; i++ )
                {
                    int byteLength = buffer.getInt();
                    result[i] = UTF8.decode( buffer.array(), buffer.position(), byteLength );
                    buffer.position( buffer.position() + byteLength );
                }
                return result;
            }
            else
            {
                ShortArray type = ShortArray.typeOf( typeId );
                int bitsUsedInLastByte = buffer.get();
                int requiredBits = buffer.get();
                if ( requiredBits == 0 )
                {
                    return type.createEmptyArray();
                }
                Object result;
                if ( type == ShortArray.BYTE && requiredBits == Byte.SIZE )
                {   // Optimization for byte arrays (probably large ones)
                    byte[] byteArray = new byte[buffer.limit() - buffer.position()];
                    buffer.get( byteArray );
                    result = byteArray;
                }
                else
                {   // Fallback to the generic approach, which is a slower
                    Bits bits = Bits.bitsFromBytes( buffer.array(), buffer.position() );
                    int length = ((buffer.limit() - buffer.position()) * 8 - (8 - bitsUsedInLastByte)) / requiredBits;
                    result = type.createArray( length, bits, requiredBits );
                }
                return result;
            }
        }
        finally
        {
            buffer.order( ByteOrder.LITTLE_ENDIAN );
        }
    }

    @Override
    public void dispose()
    {
        stringCursor.close();
        arrayCursor.close();
    }
}
