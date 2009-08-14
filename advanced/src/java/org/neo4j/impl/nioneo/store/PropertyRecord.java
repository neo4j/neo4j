/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PropertyRecord extends AbstractRecord
{
    private PropertyType type;
    private int keyIndexId = Record.NO_NEXT_BLOCK.intValue();
    private long propBlock = Record.NO_NEXT_BLOCK.intValue();
    private int prevProp = Record.NO_PREVIOUS_PROPERTY.intValue();
    private int nextProp = Record.NO_NEXT_PROPERTY.intValue();
    private List<DynamicRecord> valueRecords = new ArrayList<DynamicRecord>();
    private boolean isLight = false;
    private int nodeRelId = -1;
    private boolean nodeIdSet = false;

    public PropertyRecord( int id )
    {
        super( id );
    }

    public void setType( PropertyType type )
    {
        this.type = type;
    }

    public void setNodeId( int nodeId )
    {
        nodeIdSet = true;
        nodeRelId = nodeId;
    }

    public void setRelId( int relId )
    {
        nodeIdSet = false;
        nodeRelId = relId;
    }

    public int getNodeId()
    {
        if ( nodeIdSet )
        {
            return nodeRelId;
        }
        return -1;
    }

    public int getRelId()
    {
        if ( !nodeIdSet )
        {
            return nodeRelId;
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
        return type;
    }

    public int getKeyIndexId()
    {
        return keyIndexId;
    }

    public void setKeyIndexId( int keyId )
    {
        this.keyIndexId = keyId;
    }

    public long getPropBlock()
    {
        return propBlock;
    }

    public void setPropBlock( long propBlock )
    {
        this.propBlock = propBlock;
    }

    public int getPrevProp()
    {
        return prevProp;
    }

    public void setPrevProp( int prevProp )
    {
        this.prevProp = prevProp;
    }

    public int getNextProp()
    {
        return nextProp;
    }

    public void setNextProp( int nextProp )
    {
        this.nextProp = nextProp;
    }

    @Override
    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append( "PropertyRecord[" ).append( getId() ).append( "," ).append(
            inUse() ).append( "," ).append( type ).append( "," ).append(
            keyIndexId ).append( "," ).append( propBlock ).append( "," )
            .append( prevProp ).append( "," ).append( nextProp );
        buf.append( ", Value[" );
        for ( DynamicRecord record : valueRecords )
        {
            buf.append( record );
        }
        buf.append( "]]" );
        return buf.toString();
    }
}