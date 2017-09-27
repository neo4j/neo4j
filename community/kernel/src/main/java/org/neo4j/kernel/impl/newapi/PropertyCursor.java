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
package org.neo4j.kernel.impl.newapi;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.FloatValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.ShortValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

public class PropertyCursor extends PropertyRecord implements org.neo4j.internal.kernel.api.PropertyCursor
{
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private final Read read;
    private long next;
    private int block;
    ByteBuffer buffer;
    private PageCursor page;
    private PageCursor stringPage;
    private PageCursor arrayPage;

    public PropertyCursor( Read read )
    {
        super( -1 );
        this.read = read;
    }

    void init( long reference )
    {
        if ( getId() != NO_ID )
        {
            clear();
        }
        next = reference;
        block = Integer.MAX_VALUE;
        if ( page == null )
        {
            page = read.propertyPage( reference );
        }
    }

    @Override
    public boolean next()
    {
        if ( block < getNumberOfBlocks() )
        {
            if ( block == -1 )
            {
                block = 0;
            }
            else
            {
                block += type().calculateNumberOfBlocksUsed( currentBlock() );
            }
            if ( block < getNumberOfBlocks() && type() != null )
            {
                return true;
            }
        }
        if ( next == NO_ID )
        {
            return false;
        }
        read.property( this, next, page );
        next = getNextProp();
        block = -1;
        return next();
    }

    private long currentBlock()
    {
        return getBlocks()[block];
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( page != null )
        {
            page.close();
            page = null;
        }
        if ( stringPage != null )
        {
            stringPage.close();
            stringPage = null;
        }
        if ( arrayPage != null )
        {
            arrayPage.close();
            arrayPage = null;
        }
        clear();
    }

    @Override
    public int propertyKey()
    {
        return PropertyBlock.keyIndexId( currentBlock() );
    }

    @Override
    public ValueGroup propertyType()
    {
        PropertyType type = type();
        if ( type == null )
        {
            return ValueGroup.NO_VALUE;
        }
        switch ( type )
        {
        case BOOL:
            return ValueGroup.BOOLEAN;
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
            return ValueGroup.NUMBER;
        case STRING:
        case CHAR:
        case SHORT_STRING:
            return ValueGroup.TEXT;
        case SHORT_ARRAY:
        case ARRAY:
        default:
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    private PropertyType type()
    {
        return PropertyType.getPropertyTypeOrNull( currentBlock() );
    }

    @Override
    public Value propertyValue()
    {
        PropertyType type = type();
        if ( type == null )
        {
            return Values.NO_VALUE;
        }
        switch ( type )
        {
        case BOOL:
            return readBoolean();
        case BYTE:
            return readByte();
        case SHORT:
            return readShort();
        case INT:
            return readInt();
        case LONG:
            return readLong();
        case FLOAT:
            return readFloat();
        case DOUBLE:
            return readDouble();
        case CHAR:
            return readChar();
        case SHORT_STRING:
            return readShortString();
        case SHORT_ARRAY:
            return readShortArray();
        case STRING:
            return readLongString();
        case ARRAY:
            return readLongArray();
        default:
            throw new IllegalStateException( "Unsupported PropertyType: " + type.name() );
        }
    }

    private ArrayValue readLongArray()
    {
        long reference = PropertyBlock.fetchLong( currentBlock() );
        if ( arrayPage == null )
        {
            arrayPage = read.arrayPage( reference );
        }
        return read.array( this, reference, arrayPage );
    }

    private TextValue readLongString()
    {
        long reference = PropertyBlock.fetchLong( currentBlock() );
        if ( stringPage == null )
        {
            stringPage = read.stringPage( reference );
        }
        return read.string( this, reference, stringPage );
    }

    private Value readShortArray()
    {
        Bits bits = Bits.bits( MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY );
        int blocksUsed = ShortArray.calculateNumberOfBlocksUsed( currentBlock() );
        for ( int i = 0; i < blocksUsed; i++ )
        {
            bits.put( getBlocks()[block + i] );
        }
        return ShortArray.decode( bits );
    }

    private TextValue readShortString()
    {
        return LongerShortString
                .decode( getBlocks(), block, LongerShortString.calculateNumberOfBlocksUsed( currentBlock() ) );
    }

    private TextValue readChar()
    {
        return Values.charValue( (char) PropertyBlock.fetchShort( currentBlock() ) );
    }

    private DoubleValue readDouble()
    {
        return Values.doubleValue( Double.longBitsToDouble( getBlocks()[block + 1] ) );
    }

    private FloatValue readFloat()
    {
        return Values.floatValue( Float.intBitsToFloat( PropertyBlock.fetchInt( currentBlock() ) ) );
    }

    private LongValue readLong()
    {
        if ( PropertyBlock.valueIsInlined( currentBlock() ) )
        {
            return Values.longValue( PropertyBlock.fetchLong( currentBlock() ) >>> 1 );
        }
        else
        {
            return Values.longValue( getBlocks()[block + 1] );
        }
    }

    private IntValue readInt()
    {
        return Values.intValue( PropertyBlock.fetchInt( currentBlock() ) );
    }

    private ShortValue readShort()
    {
        return Values.shortValue( PropertyBlock.fetchShort( currentBlock() ) );
    }

    private ByteValue readByte()
    {
        return Values.byteValue( PropertyBlock.fetchByte( currentBlock() ) );
    }

    private BooleanValue readBoolean()
    {
        return Values.booleanValue( PropertyBlock.fetchByte( currentBlock() ) == 1 );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> target )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean booleanValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public String stringValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long longValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public double doubleValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( long value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( double value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( String value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueMatches( Pattern regex )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
