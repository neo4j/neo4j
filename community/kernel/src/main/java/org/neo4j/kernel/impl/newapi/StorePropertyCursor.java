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
package org.neo4j.kernel.impl.newapi;

import java.nio.ByteBuffer;
import java.util.function.IntPredicate;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.TemporalType;
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
import org.neo4j.values.storable.Values;

public class StorePropertyCursor extends PropertyRecord
{
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INITIAL_POSITION = -1;
    private Read read;
    private long next;
    private int block;
    ByteBuffer buffer;
    private PageCursor page;
    private PageCursor stringPage;
    private PageCursor arrayPage;

    public StorePropertyCursor()
    {
        super( NO_ID );
    }

    void init( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            clear();
        }

        //Set to high value to force a read
        this.block = Integer.MAX_VALUE;
        this.read = read;
        if ( reference != NO_ID )
        {
            if ( page == null )
            {
                page = read.propertyPage( reference );
            }
        }

        // Store state
        this.next = reference;
    }

    boolean next( IntPredicate filter )
    {
        while ( true )
        {
            //Figure out number of blocks of record
            int numberOfBlocks = getNumberOfBlocks();
            while ( block < numberOfBlocks )
            {
                //We have just read a record, so we are at the beginning
                if ( block == INITIAL_POSITION )
                {
                    block = 0;
                }
                else
                {
                    //Figure out the type and how many blocks that are used
                    long current = currentBlock();
                    PropertyType type = PropertyType.getPropertyTypeOrNull( current );
                    if ( type == null )
                    {
                        break;
                    }
                    block += type.calculateNumberOfBlocksUsed( current );
                }
                //nothing left, need to read a new record
                if ( block >= numberOfBlocks || type() == null )
                {
                    break;
                }

                if ( !filter.test( propertyKey() ) )
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
            block = INITIAL_POSITION;
        }
    }

    private long currentBlock()
    {
        return getBlocks()[block];
    }

    public void close()
    {
        if ( !isClosed() )
        {
            read = null;
            clear();
        }
    }

    public int propertyKey()
    {
        return PropertyBlock.keyIndexId( currentBlock() );
    }

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

    public Value propertyValue()
    {
        return readValue();
    }

    private Value readValue()
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
        case GEOMETRY:
            return geometryValue();
        case TEMPORAL:
            return temporalValue();
        default:
            throw new IllegalStateException( "Unsupported PropertyType: " + type.name() );
        }
    }

    private Value geometryValue()
    {
        return GeometryType.decode( getBlocks(), block );
    }

    private Value temporalValue()
    {
        return TemporalType.decode( getBlocks(), block );
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

    public boolean isClosed()
    {
        return read == null;
    }

    public String toString()
    {
        if ( isClosed() )
        {
            return "PropertyCursor[closed state]";
        }
        else
        {
            return "PropertyCursor[id=" + getId() + ", open state with: block=" + block + ", next=" + next +
                   ", underlying record=" + super.toString() + " ]";
        }
    }

    public void release()
    {
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
        if ( page != null )
        {
            page.close();
            page = null;
        }
    }
}
