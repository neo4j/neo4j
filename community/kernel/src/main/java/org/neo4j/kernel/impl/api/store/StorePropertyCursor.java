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
package org.neo4j.kernel.impl.api.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.cursor.Cursor;
import org.neo4j.cursor.GenericCursor;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.Record;
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
 * Cursor for all properties on a node or relationship.
 */
public class StorePropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private static final long KEY_BITMASK = 0xFFFFFFL;
    private static final int BITS_BYTE_SIZE = 32;
    private static final int INTERNAL_BYTE_ARRAY_SIZE = 4096;

    private final ByteBuffer cachedBuffer = ByteBuffer.allocate( INTERNAL_BYTE_ARRAY_SIZE );
    private final PropertyStore propertyStore;
    private final Consumer<StorePropertyCursor> instanceCache;
    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;

    private long nextPropertyRecordId;
    private PageCursor cursor;

    private int offsetAtBeginning;
    private int blocks;
    private int remainingBlocksToRead;
    private long header;
    private PropertyType type;
    private int keyId;
    private ByteBuffer buffer;

    private AbstractDynamicStore.DynamicRecordCursor recordCursor;
    private long currentRecordId;
    private long[] valuesAfterHeader;
    private boolean seekingForFirstBlock;

    public StorePropertyCursor( PropertyStore propertyStore, Consumer<StorePropertyCursor> instanceCache )
    {
        this.propertyStore = propertyStore;
        this.instanceCache = instanceCache;
        this.stringStore = propertyStore.getStringStore();
        this.arrayStore = propertyStore.getArrayStore();
    }

    public StorePropertyCursor init( long firstPropertyId )
    {
        assert cursor == null;
        assert recordCursor == null;
        assert buffer == null;
        assert type == null;

        buffer = cachedBuffer;
        nextPropertyRecordId = firstPropertyId;
        blocks = 0;
        remainingBlocksToRead = 0;
        seekingForFirstBlock = false;

        return this;
    }

    @Override
    public boolean next()
    {
        do
        {
            if ( cursor == null )
            {
                if ( nextPropertyRecordId == Record.NO_NEXT_PROPERTY.intValue() )
                {
                    return false;
                }

                nextRecord();
            }

        }
        while ( !nextBlock() );

        return true;
    }

    @Override
    public PropertyItem get()
    {
        return this;
    }

    private void readPropertyData() throws IOException
    {
        switch ( type )
        {
        case STRING:
            readFromStore( stringStore );
            break;
        case ARRAY:
            readFromStore( arrayStore );
            break;
        case SHORT_STRING:
        case SHORT_ARRAY:
            ensureValuesLoaded();
            break;

        default:
            throw new IllegalStateException();
        }
    }

    private void ensureValuesLoaded()
    {
        if ( remainingBlocksToRead > 0 )
        {
            if ( valuesAfterHeader == null || valuesAfterHeader.length < remainingBlocksToRead )
            {
                valuesAfterHeader = new long[remainingBlocksToRead];
            }

            int offset = cursor.getOffset();
            try
            {
                do
                {
                    cursor.setOffset( offset );
                    for ( int i = 0; i < blocks; i++ )
                    {
                        valuesAfterHeader[i] = cursor.getLong();
                    }
                }
                while ( cursor.shouldRetry() );
                remainingBlocksToRead -= blocks;
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    private void readFromStore( AbstractDynamicStore store ) throws IOException
    {
        int storeOffset = cursor.getOffset();
        cursor.close();
        cursor = null;

        if ( recordCursor == null )
        {
            recordCursor = store.newDynamicRecordCursor();
        }

        buffer.clear();
        long startBlockId = PropertyBlock.fetchLong( header );
        try ( GenericCursor<DynamicRecord> records = store.getRecordsCursor( startBlockId, true, recordCursor ) )
        {
            while ( records.next() )
            {
                DynamicRecord dynamicRecord = records.get();
                byte[] data = dynamicRecord.getData();
                if ( buffer.remaining() < data.length )
                {
                    buffer.flip();
                    ByteBuffer newBuffer =
                            ByteBuffer.allocate( newCapacity( data.length ) ).order( ByteOrder.LITTLE_ENDIAN );
                    newBuffer.put( buffer );
                    buffer = newBuffer;
                }
                buffer.put( data, 0, data.length );
            }
        }

        cursor = propertyStore.newReadCursor( currentRecordId );
        cursor.setOffset( storeOffset );
    }

    private int newCapacity( int required )
    {
        int newCapacity;
        do
        {
            newCapacity = buffer.capacity() * 2;
        }
        while ( newCapacity - buffer.limit()  < required );
        return newCapacity;
    }


    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }

        type = null;
        valuesAfterHeader = null;
        recordCursor = null;
        buffer = null;

        instanceCache.accept( this );
    }

    private void nextRecord()
    {
        try
        {
            cursor = propertyStore.newReadCursor( nextPropertyRecordId );
            currentRecordId = nextPropertyRecordId;

            offsetAtBeginning = cursor.getOffset();
            byte modifiers;
            long nextProp;
            do
            {
                cursor.setOffset( offsetAtBeginning );
                modifiers = cursor.getByte();
                // We don't care about previous pointer (prevProp)
                cursor.getUnsignedInt();
                nextProp = cursor.getUnsignedInt();

            }
            while ( cursor.shouldRetry() );

            long nextMod = (modifiers & 0x0FL) << 32;
            nextPropertyRecordId = longFromIntAndMod( nextProp, nextMod );
            seekingForFirstBlock = true;
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private boolean nextBlock()
    {
        // Skip remaining data from previous property (if it was not read)
        if (remainingBlocksToRead > 0)
        {
            cursor.setOffset( cursor.getOffset() + remainingBlocksToRead * 8 );
            remainingBlocksToRead = 0;
        }

        int offset = cursor.getOffset();
        if ( offset - offsetAtBeginning < PropertyStore.RECORD_SIZE )
        {
            try
            {
                do
                {
                    cursor.setOffset( offset );
                    header = cursor.getLong();
                }
                while ( cursor.shouldRetry() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }

            type = PropertyType.getPropertyType( header, true );
            if ( type != null )
            {
                seekingForFirstBlock = false;
                keyId = (int) (header & KEY_BITMASK);
                blocks = remainingBlocksToRead = type.calculateNumberOfBlocksUsed( header ) - 1;
                return true;
            }
        }

        if ( seekingForFirstBlock )
        {
            throw new NotFoundException( "Property record with id " + currentRecordId + " not in use" );
        }
        else
        {
            cursor.close();
            cursor = null;
            return false;
        }
    }

    private long longFromIntAndMod( long base, long modifier )
    {
        return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    private Object getRightArray()
    {
        buffer.flip();
        assert buffer.limit() > 0 : "buffer is empty";
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

    public int propertyKeyId()
    {
        return keyId;
    }

    @Override
    public Object value()
    {
        switch ( type )
        {
        case BOOL:
            return parseBooleanValue();
        case BYTE:
            return parseByteValue();
        case SHORT:
            return parseShortValue();
        case CHAR:
            return parseCharValue();
        case INT:
            return parseIntValue();
        case LONG:
            return parseLongValue();
        case FLOAT:
            return parseFloatValue();
        case DOUBLE:
            return parseDoubleValue();
        case SHORT_STRING:
        case STRING:
            return parseStringValue();
        case SHORT_ARRAY:
        case ARRAY:
            return parseArrayValue();
        default:
            throw new IllegalStateException( "No such type:" + type );
        }
    }

    private boolean parseBooleanValue()
    {
        assertReadingStatus();
        assertOfType( BOOL );
        return PropertyBlock.fetchByte( header ) == 1;
    }

    private byte parseByteValue()
    {
        assertReadingStatus();
        assertOfType( BYTE );
        return PropertyBlock.fetchByte( header );
    }

    private short parseShortValue()
    {
        assertReadingStatus();
        assertOfType( SHORT );
        return PropertyBlock.fetchShort( header );
    }

    private int parseIntValue()
    {
        assertReadingStatus();
        assertOfType( INT );
        return PropertyBlock.fetchInt( header );
    }

    private long parseLongValue()
    {
        assertReadingStatus();
        assertOfType( LONG );
        if ( PropertyBlock.valueIsInlined( header ) )
        {
            return PropertyBlock.fetchLong( header ) >>> 1;
        }
        else
        {
            ensureValuesLoaded();
            return valuesAfterHeader[0];
        }
    }

    private float parseFloatValue()
    {
        assertReadingStatus();
        assertOfType( FLOAT );
        return Float.intBitsToFloat( PropertyBlock.fetchInt( header ) );
    }

    private double parseDoubleValue()
    {
        assertReadingStatus();
        assertOfType( DOUBLE );
        ensureValuesLoaded();
        return Double.longBitsToDouble( valuesAfterHeader[0] );
    }

    private char parseCharValue()
    {
        assertReadingStatus();
        assertOfType( CHAR );
        return (char) PropertyBlock.fetchShort( header );
    }

    private String parseStringValue()
    {
        assertReadingStatus();
        assertOfOneOfTypes( SHORT_STRING, STRING );
        try
        {
            readPropertyData();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        if ( type == SHORT_STRING )
        {
            return LongerShortString.decode( getBitsFromLongs() );
        }
        else // STRING
        {
            buffer.flip();
            return UTF8.decode( buffer.array(), 0, buffer.limit() );
        }
    }

    private Object parseArrayValue()
    {
        assertReadingStatus();
        assertOfOneOfTypes( SHORT_ARRAY, ARRAY );
        try
        {
            readPropertyData();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
        if ( type == SHORT_ARRAY )
        {
            return ShortArray.decode( getBitsFromLongs() );
        }
        else
        {
            return getRightArray();
        }
    }

    private Bits getBitsFromLongs()
    {
        Bits bits = Bits.bits( BITS_BYTE_SIZE );
        bits.put( header );
        if ( valuesAfterHeader != null )
        {
            for ( int i = 0; i < blocks; i++ )
            {
                bits.put( valuesAfterHeader[i] );
            }
        }
        return bits;
    }

    private void assertReadingStatus()
    {
        if ( type == null )
        {
            throw new IllegalStateException();
        }
    }

    private void assertOfType( PropertyType type )
    {
        if ( this.type != type )
        {
            throw new IllegalStateException( "Expected type " + type + " but was " + this.type );
        }
    }

    private void assertOfOneOfTypes( PropertyType type1, PropertyType type2 )
    {
        if ( this.type != type1 && this.type != type2 )
        {
            throw new IllegalStateException( "Expected type " + type1 + " or " + type2 + " but was " + this.type );
        }
    }
}
