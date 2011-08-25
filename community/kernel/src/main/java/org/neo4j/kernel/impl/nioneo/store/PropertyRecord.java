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

import static org.neo4j.kernel.impl.nioneo.store.PropertyType.getPayloadSizeLongs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class PropertyRecord extends Abstract64BitRecord
{
    private PropertyType type;
    private long[] propBlock = new long[getPayloadSizeLongs()];
    private long nextProp = Record.NO_NEXT_PROPERTY.intValue();
    private long prevProp = Record.NO_NEXT_PROPERTY.intValue();
    private List<DynamicRecord> valueRecords = new ArrayList<DynamicRecord>();
    private boolean isLight;
    private long entityId = -1;
    private boolean nodeIdSet;
    private boolean isChanged;

    public PropertyRecord( long id )
    {
        super( id );
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

    public long getNodeId()
    {
        if ( nodeIdSet )
        {
            return entityId;
        }
        return -1;
    }

    public long getRelId()
    {
        if ( !nodeIdSet )
        {
            return entityId;
        }
        return -1;
    }

    void setIsLight( boolean status )
    {
        isLight = status;
    }

    public boolean isLight()
    {
        return isLight;
    }

    public Collection<DynamicRecord> getValueRecords()
    {
        assert !isLight;
        return valueRecords;
    }

    public void addValueRecord( DynamicRecord record )
    {
        assert !isLight;
        valueRecords.add( record );
    }

    public PropertyType getType()
    {
        return PropertyType.getPropertyType( propBlock[0], false );
    }

    public int getKeyIndexId()
    {
        // [kkkk,kkkk][kkkk,kkkk][kkkk,kkk ],[][][][][]
        return (int) ( propBlock[0] >> 40 );
    }

//    public void setKeyIndexId( int keyId )
//    {
//        this.keyIndexId = keyId;
//    }

    public long[] getPropBlock()
    {
        return propBlock;
    }

    public long getSinglePropBlock()
    {
        return propBlock[0];
    }

    public void setPropBlock( long[] propBlock )
    {
        this.propBlock = propBlock.length < PropertyType.getPayloadSizeLongs() ?
                Arrays.copyOf( propBlock, PropertyType.getPayloadSizeLongs() ) : propBlock;
    }

    public void setSinglePropBlock( long propBlock )
    {
        this.propBlock[0] = propBlock;
        for ( int i = 1; i < this.propBlock.length; i++ )
        {
            this.propBlock[i] = 0;
        }
    }

    public long getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( long nextProp )
    {
        this.nextProp = nextProp;
    }

    public PropertyData newPropertyData()
    {
        return getType().newPropertyData( this, null );
    }

    public PropertyData newPropertyData( Object extractedValue )
    {
        return getType().newPropertyData( this, extractedValue );
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "PropertyRecord[" ).append( getId() ).append( "," ).append(
            inUse() ).append( "," ).append( type ).append( "," ).append( propBlock ).append( "," )
            .append( "," ).append( nextProp );
        buf.append( ", Value[" );
        for ( DynamicRecord record : valueRecords )
        {
            buf.append( record );
        }
        buf.append( "]]" );
        return buf.toString();
    }

    public boolean isChanged()
    {
        return isChanged;
    }

    public void setChanged()
    {
        isChanged = true;
    }

    public long getPrevProp()
    {
        return prevProp;
    }

    public void setPrevProp( long prev )
    {
        prevProp = prev;
    }
}