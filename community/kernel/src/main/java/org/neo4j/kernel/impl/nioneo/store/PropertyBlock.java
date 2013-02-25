/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PropertyBlock
{
    private static final int MAX_ARRAY_TOSTRING_SIZE = 4;
    private final List<DynamicRecord> valueRecords = new LinkedList<DynamicRecord>();
    private long[] valueBlocks;
    private boolean isCreated;
    private boolean isChanged;

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
        return (int) (valueBlocks[0]&0xFFFFFF);
    }

    public void setSingleBlock( long value )
    {
        valueBlocks = new long[1];
        valueBlocks[0] = value;
        valueRecords.clear();
    }

    public void addValueRecord( DynamicRecord record )
    {
        valueRecords.add( record );
    }

    public List<DynamicRecord> getValueRecords()
    {
        return valueRecords;
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
        return valueRecords.isEmpty();
    }

    public PropertyData newPropertyData( PropertyRecord parent )
    {
        return newPropertyData( parent, null );
    }

    public PropertyData newPropertyData( PropertyRecord parent,
            Object extractedValue )
    {
        return getType().newPropertyData( this, parent.getId(), extractedValue );
    }

    public void setValueBlocks( long[] blocks )
    {
        assert ( blocks == null || blocks.length <= PropertyType.getPayloadSizeLongs() ) : ( "i was given an array of size " + blocks.length );
        this.valueBlocks = blocks;
        valueRecords.clear();
    }

    public boolean isCreated()
    {
        return isCreated;
    }

    public void setCreated()
    {
        isCreated = true;
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
        result.append( type == null ? "<unknown type>" : type.name() ).append( ',' );
        result.append( "key=" ).append( valueBlocks == null ? "?" : Integer.toString( getKeyIndexId() ) );
        if ( type != null ) switch ( type )
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
                    if ( i != 0 ) buf.append( "," );
                    buf.append( Array.get( value, i ) );
                }
                if ( length > MAX_ARRAY_TOSTRING_SIZE ) buf.append( ",..." );
                value = buf.append( "]" );
            }
            result.append( ",value=" ).append( value );
            break;
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
        result.isCreated = isCreated;
        if ( valueBlocks != null )
            result.valueBlocks = valueBlocks.clone();
        for ( DynamicRecord valueRecord : valueRecords )
            result.valueRecords.add( valueRecord.clone() );
        return result;
    }
    
    public boolean hasSameContentsAs( PropertyBlock other )
    {
        // Assumption (which happens to be true) that if a heavy (long string/array) property
        // changes it will get another id, making the valueBlocks values differ.
        return Arrays.equals( valueBlocks, other.valueBlocks );
    }
    
    public void setChanged( boolean isChanged )
    {
        this.isChanged = isChanged;
    }
    
    public boolean isChanged()
    {
        return isChanged;
    }
}
