/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PropertyBlock
{
    private final List<DynamicRecord> valueRecords = new LinkedList<DynamicRecord>();
    private long[] valueBlocks;
    // private boolean inUse;
    private boolean isCreated;

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
        return valueBlocks == null ? null : PropertyType.getPropertyType( valueBlocks[0], false );
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
        return valueRecords.size() == 0;
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

    /*
    public boolean inUse()
    {
        return inUse;
    }

    public void setInUse( boolean inUse )
    {
        this.inUse = inUse;
    }
    */

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
        StringBuffer result = new StringBuffer("PropertyBlock[");
        // result.append( inUse() ? "inUse, " : "notInUse, " );
        result.append( valueBlocks == null ? -1 : getKeyIndexId() ).append(
                ", " ).append( getType() );
        result.append( ", " ).append(
                valueBlocks == null ? "null" : "blocks[" + valueBlocks.length
                                               + "]" ).append(
                ", " );
        result.append( "ValueRecords[" );
        if ( !isLight() )
        {
            Iterator<DynamicRecord> recIt = valueRecords.iterator();
            while ( recIt.hasNext() )
            {
                result.append( recIt.next() );
                if ( recIt.hasNext() )
                {
                    result.append( ", " );
                }
            }
        }
        else
        {
            result.append( "<none>" );
        }

        result.append( "]]" );
        return result.toString();
    }
}
