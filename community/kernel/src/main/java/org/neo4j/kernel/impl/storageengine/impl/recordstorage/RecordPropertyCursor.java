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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.kernel.impl.store.LongerShortString;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.ShortArray;
import org.neo4j.kernel.impl.store.TemporalType;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.util.Bits;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.string.UTF8;
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

class RecordPropertyCursor extends PropertyRecord implements StoragePropertyCursor
{
    private static final int MAX_BYTES_IN_SHORT_STRING_OR_SHORT_ARRAY = 32;
    private static final int INITIAL_POSITION = -1;

    private final PropertyStore read;
    private long next;
    private int block;
    public ByteBuffer buffer;
    private PageCursor page;
    private PageCursor stringPage;
    private PageCursor arrayPage;
    private boolean open;

    RecordPropertyCursor( PropertyStore read )
    {
        super( NO_ID );
        this.read = read;
    }

    @Override
    public void init( long reference )
    {
        if ( getId() != NO_ID )
        {
            clear();
        }

        //Set to high value to force a read
        this.block = Integer.MAX_VALUE;
        if ( reference != NO_ID )
        {
            if ( page == null )
            {
                page = propertyPage( reference );
            }
        }

        // Store state
        this.next = reference;
        this.open = true;
    }

    @Override
    public boolean next()
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

                return true;
            }

            if ( next == NO_ID )
            {
                return false;
            }

            property( this, next, page );
            next = getNextProp();
            block = INITIAL_POSITION;
        }
    }

    private long currentBlock()
    {
        return getBlocks()[block];
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            clear();
        }
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
        case TEMPORAL:
        case GEOMETRY:
        case SHORT_ARRAY:
        case ARRAY:
            // value read is needed to get correct value group since type is not fine grained enough to match all ValueGroups
            return propertyValue().valueGroup();
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
            arrayPage = arrayPage( reference );
        }
        return array( this, reference, arrayPage );
    }

    private TextValue readLongString()
    {
        long reference = PropertyBlock.fetchLong( currentBlock() );
        if ( stringPage == null )
        {
            stringPage = stringPage( reference );
        }
        return string( this, reference, stringPage );
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

    public String toString()
    {
        if ( !open )
        {
            return "PropertyCursor[closed state]";
        }
        else
        {
            return "PropertyCursor[id=" + getId() + ", open state with: block=" + block + ", next=" + next +
                   ", underlying record=" + super.toString() + "]";
        }
    }

    @Override
    public void close()
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

    private PageCursor propertyPage( long reference )
    {
        return read.openPageCursorForReading( reference );
    }

    private PageCursor stringPage( long reference )
    {
        return read.openStringPageCursor( reference );
    }

    private PageCursor arrayPage( long reference )
    {
        return read.openArrayPageCursor( reference );
    }

    private void property( PropertyRecord record, long reference, PageCursor pageCursor )
    {
        // We need to load forcefully here since otherwise we can have inconsistent reads
        // for properties across blocks, see org.neo4j.graphdb.ConsistentPropertyReadsIT
        read.getRecordByCursor( reference, record, RecordLoad.FORCE, pageCursor );
    }

    private TextValue string( RecordPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = read.loadString( reference, cursor.buffer, page );
        buffer.flip();
        return Values.stringValue( UTF8.decode( buffer.array(), 0, buffer.limit() ) );
    }

    private ArrayValue array( RecordPropertyCursor cursor, long reference, PageCursor page )
    {
        ByteBuffer buffer = cursor.buffer = read.loadArray( reference, cursor.buffer, page );
        buffer.flip();
        return PropertyStore.readArrayFromBuffer( buffer );
    }
}
