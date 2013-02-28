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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.kernel.impl.api.UpdateMode;

public class NodePropertyUpdate
{
    private final long nodeId;
    private final long propertyKeyId;
    private final Object valueBefore;
    private final Object valueAfter;
    private final UpdateMode updateMode;

    public NodePropertyUpdate( long nodeId, long propertyKeyId, Object valueBefore, Object valueAfter )
    {
        this.nodeId = nodeId;
        this.propertyKeyId = propertyKeyId;
        this.valueBefore = valueBefore;
        this.valueAfter = valueAfter;
        this.updateMode = figureOutUpdateMode( valueBefore, valueAfter );
    }

    private UpdateMode figureOutUpdateMode( Object valueBefore, Object valueAfter )
    {
        boolean beforeSet = valueBefore != null;
        boolean afterSet = valueAfter != null;
        if ( !beforeSet && afterSet )
            return UpdateMode.ADDED;
        if ( beforeSet && afterSet )
            return UpdateMode.CHANGED;
        if ( beforeSet && !afterSet )
            return UpdateMode.REMOVED;
        throw new IllegalArgumentException( "Neither before or after set" );
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public long getPropertyKeyId()
    {
        return propertyKeyId;
    }

    public Object getValueBefore()
    {
        return valueBefore;
    }

    public Object getValueAfter()
    {
        return valueAfter;
    }

    public UpdateMode getUpdateMode()
    {
        return updateMode;
    }

    public boolean hasLabel( long labelId )
    {
        // TODO implement adding label id info
        return true;
    }
    
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + nodeId + ", prop:" + propertyKeyId + ", before:" + valueBefore +
                ", after:" + valueAfter + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (nodeId ^ (nodeId >>> 32));
        result = prime * result + (int) (propertyKeyId ^ (propertyKeyId >>> 32));
        result = prime * result + ((updateMode == null) ? 0 : updateMode.hashCode());
        result = prime * result + ((valueAfter == null) ? 0 : valueAfter.hashCode());
        result = prime * result + ((valueBefore == null) ? 0 : valueBefore.hashCode());
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        NodePropertyUpdate other = (NodePropertyUpdate) obj;
        if ( nodeId != other.nodeId )
            return false;
        if ( propertyKeyId != other.propertyKeyId )
            return false;
        if ( updateMode != other.updateMode )
            return false;
        if ( valueAfter == null )
        {
            if ( other.valueAfter != null )
                return false;
        }
        else if ( !valueAfter.equals( other.valueAfter ) )
            return false;
        if ( valueBefore == null )
        {
            if ( other.valueBefore != null )
                return false;
        }
        else if ( !valueBefore.equals( other.valueBefore ) )
            return false;
        return true;
    }
}
