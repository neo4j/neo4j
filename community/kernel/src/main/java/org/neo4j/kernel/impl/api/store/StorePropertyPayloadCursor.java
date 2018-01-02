/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.neo4j.cursor.GenericCursor;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.util.Bits;

import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.BOOL;
import static org.neo4j.kernel.impl.store.PropertyType.BYTE;
import static org.neo4j.kernel.impl.store.PropertyType.CHAR;
import static org.neo4j.kernel.impl.store.PropertyType.DOUBLE;
import static org.neo4j.kernel.impl.store.PropertyType.FLOAT;
import static org.neo4j.kernel.impl.store.PropertyType.INT;
import static org.neo4j.kernel.impl.store.PropertyType.LONG;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_STRING;
import static org.neo4j.kernel.impl.store.PropertyType.STRING;

/**
 * Cursor that provides a view on property blocks of a particular property record.
 * This cursor is reusable and can be re-initialized with
 * {@link #init(PageCursor)} method and cleaned up using {@link #clear()} method.
 * <p/>
 * During initialization {@link #MAX_NUMBER_OF_PAYLOAD_LONG_ARRAY} number of longs is read from
 * the given {@linkplain PageCursor}. This is done eagerly to avoid reading property blocks from different versions
 * of the page.
 * <p/>
 * Internally, this cursor is mainly an array of {@link #MAX_NUMBER_OF_PAYLOAD_LONG_ARRAY} and a current-position
 * pointer.
 */
class StorePropertyPayloadCursor
{
    static final int MAX_NUMBER_OF_PAYLOAD_LONG_ARRAY = PropertyStore.DEFAULT_PAYLOAD_SIZE / 8;

    private static final long PROPERTY_KEY_ID_BITMASK = 0xFFFFFFL;
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INTERNAL_BYTE_ARRAY_SIZE = 4096;
    private static final int INITIAL_POSITION = -1;

    /**
     * Reusable initial buffer for reading of dynamic records.
     */
    private final ByteBuffer cachedBuffer = ByteBuffer.allocate( INTERNAL_BYTE_ARRAY_SIZE );

    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;

    private AbstractDynamicStore.DynamicRecordCursor stringRecordCursor;
    private AbstractDynamicStore.DynamicRecordCursor arrayRecordCursor;
    private ByteBuffer buffer = cachedBuffer;

    private final long[] data = new long[MAX_NUMBER_OF_PAYLOAD_LONG_ARRAY];
    private int position = INITIAL_POSITION;
    private boolean exhausted;

    StorePropertyPayloadCursor( DynamicStringStore stringStore, DynamicArrayStore arrayStore )
    {
        this.stringStore = stringStore;
        this.arrayStore = arrayStore;
    }

    void init( PageCursor cursor )
    {
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = cursor.getLong();
        }
    }

    void clear()
    {
        position = INITIAL_POSITION;
        exhausted = false;
        buffer = cachedBuffer;
        // Array of data should be filled with '0' because it is possible to call next() without calling init().
        // In such case 'false' should be returned, which might not be the case if there is stale data in the buffer.
        Arrays.fill( data, 0 );
    }

    boolean next()
    {
        if ( exhausted )
        {
            return false;
        }

        if ( position == INITIAL_POSITION )
        {
            position = 0;
        }
        else
        {
            position += currentBlocksUsed();
        }

        if ( position >= data.length || type() == null )
        {
            exhausted = true;
            return false;
        }
        return true;
    }

    PropertyType type()
    {
        return PropertyType.getPropertyType( currentHeader(), true );
    }

    int propertyKeyId()
    {
        return (int) (currentHeader() & PROPERTY_KEY_ID_BITMASK);
    }

    boolean booleanValue()
    {
        assertOfType( BOOL );
        return PropertyBlock.fetchByte( currentHeader() ) == 1;
    }

    byte byteValue()
    {
        assertOfType( BYTE );
        return PropertyBlock.fetchByte( currentHeader() );
    }

    short shortValue()
    {
        assertOfType( SHORT );
        return PropertyBlock.fetchShort( currentHeader() );
    }

    char charValue()
    {
        assertOfType( CHAR );
        return (char) PropertyBlock.fetchShort( currentHeader() );
    }

    int intValue()
    {
        assertOfType( INT );
        return PropertyBlock.fetchInt( currentHeader() );
    }

    float floatValue()
    {
        assertOfType( FLOAT );
        return Float.intBitsToFloat( PropertyBlock.fetchInt( currentHeader() ) );
    }

    long longValue()
    {
        assertOfType( LONG );
        if ( PropertyBlock.valueIsInlined( currentHeader() ) )
        {
            return PropertyBlock.fetchLong( currentHeader() ) >>> 1;
        }

        return data[position + 1];
    }

    double doubleValue()
    {
        assertOfType( DOUBLE );
        return Double.longBitsToDouble( data[position + 1] );
    }

    String shortStringValue()
    {
        assertOfType( SHORT_STRING );
        return LongerShortString.decode( data, position, currentBlocksUsed() );
    }

    String stringValue()
    {
        assertOfType( STRING );
        try
        {
            if ( stringRecordCursor == null )
            {
                stringRecordCursor = stringStore.newDynamicRecordCursor();
            }
            readFromStore( stringStore, stringRecordCursor );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to read string value", e );
        }
        buffer.flip();
        return UTF8.decode( buffer.array(), 0, buffer.limit() );
    }

    Object shortArrayValue()
    {
        assertOfType( SHORT_ARRAY );
        Bits bits = valueAsBits();
        return ShortArray.decode( bits );
    }

    Object arrayValue()
    {
        assertOfType( ARRAY );
        try
        {
            if ( arrayRecordCursor == null )
            {
                arrayRecordCursor = arrayStore.newDynamicRecordCursor();
            }
            readFromStore( arrayStore, arrayRecordCursor );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to read array value", e );
        }
        buffer.flip();
        return readArrayFromBuffer( buffer );
    }

    private long currentHeader()
    {
        return data[position];
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
            bits.put( data[position + i] );
        }
        return bits;
    }

    private void readFromStore( AbstractDynamicStore store, AbstractDynamicStore.DynamicRecordCursor cursor )
            throws IOException
    {
        buffer.clear();
        long startBlockId = PropertyBlock.fetchLong( currentHeader() );
        try ( GenericCursor<DynamicRecord> records = store.getRecordsCursor( startBlockId, cursor ) )
        {
            while ( records.next() )
            {
                DynamicRecord dynamicRecord = records.get();
                byte[] data = dynamicRecord.getData();
                if ( buffer.remaining() < data.length )
                {
                    buffer.flip();
                    ByteBuffer newBuffer = newBiggerBuffer( data.length );
                    newBuffer.put( buffer );
                    buffer = newBuffer;
                }
                buffer.put( data, 0, data.length );
            }
        }
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

    private void assertOfType( PropertyType expected )
    {
        assert type() == expected : "Expected type " + expected + " but was " + type();
    }
}
