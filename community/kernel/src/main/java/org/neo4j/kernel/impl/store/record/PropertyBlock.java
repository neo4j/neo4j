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
package org.neo4j.kernel.impl.store.record;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;

import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

public class PropertyBlock implements Cloneable
{
    private static final long KEY_BITMASK = 0xFFFFFFL;

    private static final int MAX_ARRAY_TOSTRING_SIZE = 4;
    private List<DynamicRecord> valueRecords;
    private long[] valueBlocks;

    public PropertyType getType()
    {
        return getType( false );
    }

    public PropertyType forceGetType()
    {
        return getType( true );
    }

    private PropertyType getType( boolean force )
    {
        return valueBlocks == null ? null : PropertyType.getPropertyType( valueBlocks[0], force );
    }

    public int getKeyIndexId()
    {
        // [][][][][][kkkk,kkkk][kkkk,kkkk][kkkk,kkkk]
        return (int) (valueBlocks[0] & KEY_BITMASK);
    }

    public void setKeyIndexId( int key )
    {
        valueBlocks[0] &= ~KEY_BITMASK;
        valueBlocks[0] |= key;
    }

    public void setSingleBlock( long value )
    {
        valueBlocks = new long[1];
        valueBlocks[0] = value;
        if ( valueRecords != null )
        {
            valueRecords.clear();
        }
    }

    public void addValueRecord( DynamicRecord record )
    {
        if ( valueRecords == null )
        {
            valueRecords = new LinkedList<>();
        }
        valueRecords.add( record );
    }

    public void setValueRecords( List<DynamicRecord> valueRecords )
    {
        assert this.valueRecords == null || this.valueRecords.isEmpty() : this.valueRecords.toString();
        this.valueRecords = valueRecords;
    }

    public List<DynamicRecord> getValueRecords()
    {
        return valueRecords != null ? valueRecords : Collections.<DynamicRecord>emptyList();
    }

    public long getSingleValueBlock()
    {
        return valueBlocks[0];
    }

    /**
     * use this for references to the dynamic stores
     */
    public long getSingleValueLong()
    {
        return (valueBlocks[0] & 0xFFFFFFFFF0000000L) >>> 28;
    }

    public int getSingleValueInt()
    {
        return (int)((valueBlocks[0] & 0x0FFFFFFFF0000000L) >>> 28);
    }

    public short getSingleValueShort()
    {
        return (short)((valueBlocks[0] & 0x00000FFFF0000000L) >>> 28);
    }

    public byte getSingleValueByte()
    {
        return (byte)((valueBlocks[0] & 0x0000000FF0000000L) >>> 28);
    }

    public long[] getValueBlocks()
    {
        return valueBlocks;
    }

    public boolean isLight()
    {
        return valueRecords == null || valueRecords.isEmpty();
    }

    public void setValueBlocks( long[] blocks )
    {
        int expectedPayloadSize = PropertyType.getPayloadSizeLongs();
        assert ( blocks == null || blocks.length <= expectedPayloadSize) : (
                "I was given an array of size " + blocks.length +", but I wanted it to be " + expectedPayloadSize );
        this.valueBlocks = blocks;
        if ( valueRecords != null )
        {
            valueRecords.clear();
        }
    }

    /**
     * A property block can take a variable size of bytes in a property record.
     * This method returns the size of this block in bytes, including the header
     * size.
     *
     * @return The size of this block in bytes, including the header.
     */
    public int getSize()
    {
        // Currently each block is a multiple of 8 in size
        return valueBlocks == null ? 0 : valueBlocks.length * 8;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder("PropertyBlock[");
        PropertyType type = getType();
        if ( valueBlocks != null )
        {
            result.append( "blocks=" ).append( valueBlocks.length ).append( "," );
        }
        result.append( type == null ? "<unknown type>" : type.name() ).append( ',' );
        result.append( "key=" ).append( valueBlocks == null ? "?" : Integer.toString( getKeyIndexId() ) );
        if ( type != null )
        {
            switch ( type )
            {
            case STRING:
            case ARRAY:
                result.append( ",firstDynamic=" ).append( getSingleValueLong() );
                break;
            default:
                Object value = type.getValue( this, null );
                if ( value != null && value.getClass().isArray() )
                {
                    int length = Array.getLength( value );
                    StringBuilder buf = new StringBuilder( value.getClass().getComponentType().getSimpleName() ).append( "[" );
                    for ( int i = 0; i < length && i <= MAX_ARRAY_TOSTRING_SIZE; i++ )
                    {
                        if ( i != 0 )
                        {
                            buf.append( "," );
                        }
                        buf.append( Array.get( value, i ) );
                    }
                    if ( length > MAX_ARRAY_TOSTRING_SIZE )
                    {
                        buf.append( ",..." );
                    }
                    value = buf.append( "]" );
                }
                result.append( ",value=" ).append( value );
                break;
            }
        }
        if ( !isLight() )
        {
            result.append( ",ValueRecords[" );
            Iterator<DynamicRecord> recIt = valueRecords.iterator();
            while ( recIt.hasNext() )
            {
                result.append( recIt.next() );
                if ( recIt.hasNext() )
                {
                    result.append( ',' );
                }
            }
            result.append( ']' );
        }
        result.append( ']' );
        return result.toString();
    }

    @Override
    public PropertyBlock clone()
    {
        PropertyBlock result = new PropertyBlock();
        if ( valueBlocks != null )
        {
            result.valueBlocks = valueBlocks.clone();
        }
        if ( valueRecords != null )
        {
            for ( DynamicRecord valueRecord : valueRecords )
            {
                result.addValueRecord( valueRecord.clone() );
            }
        }
        return result;
    }

    public boolean hasSameContentsAs( PropertyBlock other )
    {
        // Assumption (which happens to be true) that if a heavy (long string/array) property
        // changes it will get another id, making the valueBlocks values differ.
        return Arrays.equals( valueBlocks, other.valueBlocks );
    }

    public DefinedProperty newPropertyData( PropertyStore propertyStore )
    {
        return getType().readProperty( getKeyIndexId(), this, singletonProvider(propertyStore) );
    }
}
