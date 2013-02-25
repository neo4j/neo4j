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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * PropertyRecord is a container for PropertyBlocks. PropertyRecords form
 * a double linked list and each one holds one or more PropertyBlocks that
 * are the actual property key/value pairs. Because PropertyBlocks are of
 * variable length, a full PropertyRecord can be holding just one
 * PropertyBlock.
 */
public class PropertyRecord extends Abstract64BitRecord
{
    private long nextProp = Record.NO_NEXT_PROPERTY.intValue();
    private long prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
    private final List<PropertyBlock> blockRecords = new ArrayList<PropertyBlock>( 4 );
    private long entityId = -1;
    private Boolean nodeIdSet;
    private boolean isChanged;
    private final List<DynamicRecord> deletedRecords = new LinkedList<DynamicRecord>();

    public PropertyRecord( long id )
    {
        super( id );
    }

    public PropertyRecord( long id, PrimitiveRecord primitive )
    {
        super( id );
        setCreated();
        primitive.setIdTo( this );
    }

    public void setNodeId( long nodeId )
    {
        nodeIdSet = true;
        entityId = nodeId;
    }

    public void setRelId( long relId )
    {
        nodeIdSet = false;
        entityId = relId;
    }
    
    public boolean isNodeSet()
    {
        return Boolean.TRUE.equals( nodeIdSet );
    }

    public boolean isRelSet()
    {
        return Boolean.FALSE.equals( nodeIdSet );
    }
    
    public long getNodeId()
    {
        if ( isNodeSet() )
        {
            return entityId;
        }
        return -1;
    }

    public long getRelId()
    {
        if ( isRelSet() )
        {
            return entityId;
        }
        return -1;
    }

    /**
     * Gets the sum of the sizes of the blocks in this record, in bytes.
     *
     * @return
     */
    public int size()
    {
        int result = 0;
        final int size = blockRecords.size();
        for (int i = 0; i < size; i++)
        {
            result += blockRecords.get(i).getSize();
        }
        return result;
    }

    public List<PropertyBlock> getPropertyBlocks()
    {
        return blockRecords;
    }

    public List<DynamicRecord> getDeletedRecords()
    {
        return deletedRecords;
    }

    public void addDeletedRecord( DynamicRecord record )
    {
        deletedRecords.add( record );
    }

    public void addPropertyBlock(PropertyBlock block)
    {
        assert size() + block.getSize() <= PropertyType.getPayloadSize() :
            ("Exceeded capacity of property record " + this
                             + ". My current size is reported as " + size() + "The added block was " + block + " (note that size is "
          + block.getSize() + ")"
        );

        blockRecords.add( block );
    }

    public PropertyBlock getPropertyBlock( int keyIndex )
    {
        for ( PropertyBlock block : blockRecords )
        {
            if ( block.getKeyIndexId() == keyIndex )
            {
                return block;
            }
        }
        return null;
    }

    public PropertyBlock removePropertyBlock( int keyIndex )
    {
        for ( int i = 0; i < blockRecords.size(); i++ )
        {
            if ( blockRecords.get( i ).getKeyIndexId() == keyIndex )
            {
                return blockRecords.remove( i );
            }
        }
        return null;
    }

    public long getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( long nextProp )
    {
        this.nextProp = nextProp;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder();
        buf.append( "Property[" ).append( getId() ).append( ",used=" ).append( inUse() ).append( ",prev=" ).append(
                prevProp ).append( ",next=" ).append( nextProp );
        if ( entityId != -1 ) buf.append( nodeIdSet ? ",node=" : ",rel=" ).append( entityId );
        for ( PropertyBlock block : blockRecords )
        {
            buf.append( ',' ).append( block );
        }
        for ( DynamicRecord dyn : deletedRecords )
        {
            buf.append( ",del:" ).append( dyn );
        }
        buf.append( "]" );
        return buf.toString();
    }

    public boolean isChanged()
    {
        return isChanged;
    }

    public void setChanged( PrimitiveRecord primitive )
    {
        isChanged = true;
        primitive.setIdTo( this );
    }

    public long getPrevProp()
    {
        return prevProp;
    }

    public void setPrevProp( long prev )
    {
        prevProp = prev;
    }
    
    @Override
    public PropertyRecord clone()
    {
        PropertyRecord result = new PropertyRecord( getLongId() );
        result.setInUse( inUse() );
        result.nextProp = nextProp;
        result.prevProp = prevProp;
        result.entityId = entityId;
        result.nodeIdSet = nodeIdSet;
        result.isChanged = isChanged;
        for ( PropertyBlock block : blockRecords )
            result.blockRecords.add( block.clone() );
        for ( DynamicRecord deletedRecord : deletedRecords )
            result.deletedRecords.add( deletedRecord.clone() );
        return result;
    }
}
