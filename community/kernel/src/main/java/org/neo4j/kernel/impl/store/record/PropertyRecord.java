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
package org.neo4j.kernel.impl.store.record;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.api.store.PropertyBlockCursor;

/**
 * PropertyRecord is a container for PropertyBlocks. PropertyRecords form
 * a double linked list and each one holds one or more PropertyBlocks that
 * are the actual property key/value pairs. Because PropertyBlocks are of
 * variable length, a full PropertyRecord can be holding just one
 * PropertyBlock.
 */
public class PropertyRecord extends Abstract64BitRecord implements Iterable<PropertyBlock>, Iterator<PropertyBlock>
{
    private long nextProp = Record.NO_NEXT_PROPERTY.intValue();
    private long prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
    private final PropertyBlock[] blockRecords =
            new PropertyBlock[PropertyType.getPayloadSizeLongs() /*we can have at most these many*/];
    private int blockRecordsCursor;
    private long entityId = -1;
    private Boolean nodeIdSet;
    private List<DynamicRecord> deletedRecords;
    private String malformedMessage;

    // state for the Iterator aspect of this class.
    private int blockRecordsIteratorCursor;
    private boolean canRemoveFromIterator;

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
     */
    public int size()
    {
        int result = 0;
        for ( int i = 0; i < blockRecordsCursor; i++ )
        {
            result += blockRecords[i].getSize();
        }
        return result;
    }

    public int numberOfProperties()
    {
        return blockRecordsCursor;
    }

    @Override
    public Iterator<PropertyBlock> iterator()
    {
        blockRecordsIteratorCursor = 0;
        canRemoveFromIterator = false;
        return this;
    }

    @Override
    public boolean hasNext()
    {
        return blockRecordsIteratorCursor < blockRecordsCursor;
    }

    @Override
    public PropertyBlock next()
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        canRemoveFromIterator = true;
        return blockRecords[blockRecordsIteratorCursor++];
    }

    @Override
    public void remove()
    {
        if ( !canRemoveFromIterator )
        {
            throw new IllegalStateException(
                    "cursor:" + blockRecordsIteratorCursor + " canRemove:" + canRemoveFromIterator );
        }

        if ( --blockRecordsCursor > --blockRecordsIteratorCursor )
        {
            blockRecords[blockRecordsIteratorCursor] = blockRecords[blockRecordsCursor];
        }
        canRemoveFromIterator = false;
    }

    public List<DynamicRecord> getDeletedRecords()
    {
        return deletedRecords != null ? deletedRecords : Collections.<DynamicRecord>emptyList();
    }

    public void addDeletedRecord( DynamicRecord record )
    {
        assert !record.inUse();
        if ( deletedRecords == null )
        {
            deletedRecords = new LinkedList<>();
        }
        deletedRecords.add( record );
    }

    public void addPropertyBlock( PropertyBlock block )
    {
        if ( size() + block.getSize() > PropertyType.getPayloadSize() )
        {
            malformedMessage = "Exceeded capacity of PropertyRecord[" + getId() + "]. " +
                               "PropertyRecord size is " + size() + ". " +
                               "The added block of type " + block.forceGetType() + " has size " + block.getSize();
        }

        blockRecords[blockRecordsCursor++] = block;
    }

    public void verifyRecordIsWellFormed()
    {
        if ( malformedMessage != null )
        {
            throw new InvalidRecordException( malformedMessage );
        }
    }

    public void setMalformedMessage( String message )
    {
        malformedMessage = message;
    }

    public void setPropertyBlock( PropertyBlock block )
    {
        removePropertyBlock( block.getKeyIndexId() );
        addPropertyBlock( block );
    }

    public PropertyBlock getPropertyBlock( int keyIndex )
    {
        for ( int i = 0; i < blockRecordsCursor; i++ )
        {
            PropertyBlock block = blockRecords[i];
            if ( block.getKeyIndexId() == keyIndex )
            {
                return block;
            }
        }
        return null;
    }

    public PropertyBlock removePropertyBlock( int keyIndex )
    {
        for ( int i = 0; i < blockRecordsCursor; i++ )
        {
            if ( blockRecords[i].getKeyIndexId() == keyIndex )
            {
                PropertyBlock block = blockRecords[i];
                if ( --blockRecordsCursor > i )
                {
                    blockRecords[i] = blockRecords[blockRecordsCursor];
                }
                return block;
            }
        }
        return null;
    }

    public void clearPropertyBlocks()
    {
        blockRecordsCursor = 0;
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
        if ( entityId != -1 )
        {
            buf.append( nodeIdSet ? ",node=" : ",rel=" ).append( entityId );
        }
        for ( int i = 0; i < blockRecordsCursor; i++ )
        {
            buf.append( ',' ).append( blockRecords[i] );
        }
        if ( deletedRecords != null )
        {
            for ( DynamicRecord dyn : deletedRecords )
            {
                buf.append( ",del:" ).append( dyn );
            }
        }
        buf.append( "]" );
        return buf.toString();
    }

    public void setChanged( PrimitiveRecord primitive )
    {
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
        for ( int i = 0; i < blockRecordsCursor; i++ )
        {
            result.blockRecords[i] = blockRecords[i].clone();
        }
        result.blockRecordsCursor = blockRecordsCursor;
        if ( deletedRecords != null )
        {
            for ( DynamicRecord deletedRecord : deletedRecords )
            {
                result.addDeletedRecord( deletedRecord.clone() );
            }
        }
        return result;
    }

    public PropertyBlockCursor getPropertyBlockCursor(PropertyBlockCursor cursor)
    {
        cursor.init(blockRecords, blockRecordsCursor);

        return cursor;
    }
}
