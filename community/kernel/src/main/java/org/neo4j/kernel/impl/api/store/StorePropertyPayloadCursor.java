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
package org.neo4j.kernel.impl.api.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.TemporalType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.CharValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.PropertyType.ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.BOOL;
import static org.neo4j.kernel.impl.store.PropertyType.BYTE;
import static org.neo4j.kernel.impl.store.PropertyType.CHAR;
import static org.neo4j.kernel.impl.store.PropertyType.DOUBLE;
import static org.neo4j.kernel.impl.store.PropertyType.FLOAT;
import static org.neo4j.kernel.impl.store.PropertyType.GEOMETRY;
import static org.neo4j.kernel.impl.store.PropertyType.INT;
import static org.neo4j.kernel.impl.store.PropertyType.LONG;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_ARRAY;
import static org.neo4j.kernel.impl.store.PropertyType.SHORT_STRING;
import static org.neo4j.kernel.impl.store.PropertyType.STRING;
import static org.neo4j.kernel.impl.store.PropertyType.TEMPORAL;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Cursor that provides a view on property blocks of a particular property record.
 * This cursor is reusable and can be re-initialized with
 * {@link #init(long[], int)} method and cleaned up using {@link #clear()} method.
 * <p>
 * During initialization the raw property block {@code long}s are read from
 * the given property record.
 */
class StorePropertyPayloadCursor
{
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INTERNAL_BYTE_ARRAY_SIZE = 4096;
    private static final int INITIAL_POSITION = -1;

    /**
     * Reusable initial buffer for reading of dynamic records.
     */
    private final ByteBuffer cachedBuffer = ByteBuffer.allocate( INTERNAL_BYTE_ARRAY_SIZE );

    private final RecordCursor<DynamicRecord> stringRecordCursor;
    private final RecordCursor<DynamicRecord> arrayRecordCursor;
    private ByteBuffer buffer = cachedBuffer;

    private long[] data;
    private int position = INITIAL_POSITION;
    private int numberOfBlocks;
    private boolean exhausted;

    StorePropertyPayloadCursor( RecordCursor<DynamicRecord> stringRecordCursor,
            RecordCursor<DynamicRecord> arrayRecordCursor )
    {
        this.stringRecordCursor = stringRecordCursor;
        this.arrayRecordCursor = arrayRecordCursor;
    }

    void init( long[] blocks, int numberOfBlocks )
    {
        position = INITIAL_POSITION;
        buffer = cachedBuffer;
        data = blocks;
        this.numberOfBlocks = numberOfBlocks;
        exhausted = false;
    }

    void clear()
    {
        position = INITIAL_POSITION;
        numberOfBlocks = 0;
        exhausted = false;
        buffer = cachedBuffer;
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
        else if ( position < numberOfBlocks )
        {
            position += currentBlocksUsed();
        }

        if ( position >= numberOfBlocks || type() == null )
        {
            exhausted = true;
            return false;
        }
        return true;
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

    private BooleanValue booleanValue()
    {
        assertOfType( BOOL );
        return Values.booleanValue( PropertyBlock.fetchByte( currentHeader() ) == 1 );
    }

    private ByteValue byteValue()
    {
        assertOfType( BYTE );
        return Values.byteValue( PropertyBlock.fetchByte( currentHeader() ) );
    }

    private ShortValue shortValue()
    {
        assertOfType( SHORT );
        return Values.shortValue( PropertyBlock.fetchShort( currentHeader() ) );
    }

    private CharValue charValue()
    {
        assertOfType( CHAR );
        return Values.charValue( (char) PropertyBlock.fetchShort( currentHeader() ) );
    }

    private IntValue intValue()
    {
        assertOfType( INT );
        return Values.intValue( PropertyBlock.fetchInt( currentHeader() ) );
    }

    private FloatValue floatValue()
    {
        assertOfType( FLOAT );
        return Values.floatValue( Float.intBitsToFloat( PropertyBlock.fetchInt( currentHeader() ) ) );
    }

    private LongValue longValue()
    {
        assertOfType( LONG );
        if ( PropertyBlock.valueIsInlined( currentHeader() ) )
        {
            return Values.longValue( PropertyBlock.fetchLong( currentHeader() ) >>> 1 );
        }

        return Values.longValue( data[position + 1] );
    }

    private DoubleValue doubleValue()
    {
        assertOfType( DOUBLE );
        return Values.doubleValue( Double.longBitsToDouble( data[position + 1] ) );
    }

    private TextValue shortStringValue()
    {
        assertOfType( SHORT_STRING );
        return LongerShortString.decode( data, position, currentBlocksUsed() );
    }

    TextValue stringValue()
    {
        assertOfType( STRING );
        readFromStore( stringRecordCursor );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    private Value shortArrayValue()
    {
        assertOfType( SHORT_ARRAY );
        Bits bits = valueAsBits();
        return ShortArray.decode( bits );
    }

    Value arrayValue()
    {
        assertOfType( ARRAY );
        readFromStore( arrayRecordCursor );
        buffer.flip();
        return PropertyUtil.readArrayFromBuffer( buffer );
    }

    Value geometryValue()
    {
        assertOfType( GEOMETRY );
        return GeometryType.decode( data, position );
    }

    Value temporalValue()
    {
        assertOfType( TEMPORAL );
        return TemporalType.decode( data, position );
    }

    Value value()
    {
        switch ( type() )
        {
        case BOOL:
            return booleanValue();
        case BYTE:
            return byteValue();
        case SHORT:
            return shortValue();
        case CHAR:
            return charValue();
        case INT:
            return intValue();
        case LONG:
            return longValue();
        case FLOAT:
            return floatValue();
        case DOUBLE:
            return doubleValue();
        case SHORT_STRING:
            return shortStringValue();
        case STRING:
            return stringValue();
        case SHORT_ARRAY:
            return shortArrayValue();
        case ARRAY:
            return arrayValue();
        case GEOMETRY:
            return geometryValue();
        case TEMPORAL:
            return temporalValue();
        default:
            throw new IllegalStateException( "No such type:" + type() );
        }
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

    private void readFromStore( RecordCursor<DynamicRecord> cursor )
    {
        buffer.clear();
        long startBlockId = PropertyBlock.fetchLong( currentHeader() );
        cursor.placeAt( startBlockId, FORCE );
        while ( true )
        {
            cursor.next();
            DynamicRecord dynamicRecord = cursor.get();
            byte[] data = dynamicRecord.getData();
            if ( buffer.remaining() < data.length )
            {
                buffer.flip();
                ByteBuffer newBuffer = newBiggerBuffer( data.length );
                newBuffer.put( buffer );
                buffer = newBuffer;
            }
            buffer.put( data, 0, data.length );
            if ( Record.NULL_REFERENCE.is( dynamicRecord.getNextBlock() ) )
            {
                break;
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

    private void assertOfType( PropertyType expected )
    {
        assert type() == expected : "Expected type " + expected + " but was " + type();
    }
}
