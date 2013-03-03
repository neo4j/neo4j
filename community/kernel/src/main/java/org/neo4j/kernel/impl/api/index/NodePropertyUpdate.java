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

import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.api.UpdateMode;

public class NodePropertyUpdate
{
    private final long nodeId;
    private final long propertyKeyId;
    private final Object valueBefore;
    private final Object valueAfter;
    private final UpdateMode updateMode;
    private final long[] labelsBefore;
    private final long[] labelsAfter;

    /**
     * @param labels a sorted array of labels this property update is for.
     */
    public NodePropertyUpdate( long nodeId, long propertyKeyId, Object valueBefore, long[] labelsBefore,
            Object valueAfter, long[] labelsAfter )
    {
        this.nodeId = nodeId;
        this.propertyKeyId = propertyKeyId;
        this.valueBefore = valueBefore;
        this.labelsBefore = labelsBefore;
        this.valueAfter = valueAfter;
        this.labelsAfter = labelsAfter;
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

    /**
     * Whether or not this property update is for the given {@code labelId}.
     * 
     * If this property update comes from setting/changing/removing a property it will
     * affect all labels on that {@link Node}.
     * 
     * If this property update comes from adding or removing labels to/from a {@link Node}
     * it will affect only those labels.
     * 
     * @param labelId the label id the check.
     */
    public boolean forLabel( long labelId )
    {
        return updateMode.forLabel( labelsBefore, labelsAfter, labelId );
    }
    
    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( getClass().getSimpleName() + "[" + nodeId + ", prop:" + propertyKeyId + " " );
        switch ( updateMode )
        {
        case ADDED: result.append( "add:" + valueAfter ); break;
        case CHANGED: result.append( "change:" + valueBefore + " => " + valueAfter ); break;
        case REMOVED: result.append( "remove:" + valueBefore ); break;
        default: throw new IllegalArgumentException( updateMode.toString() );
        }
        result.append( ", labelsBefore:" + Arrays.toString( labelsBefore ) );
        result.append( ", labelsAfter:" + Arrays.toString( labelsAfter ) );
        return result.append( "]" ).toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode( labelsBefore );
        result = prime * result + Arrays.hashCode( labelsAfter );
        result = prime * result + (int) (nodeId ^ (nodeId >>> 32));
        result = prime * result + (int) (propertyKeyId ^ (propertyKeyId >>> 32));
        result = prime * result + updateMode.hashCode();
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
        if ( !Arrays.equals( labelsBefore, other.labelsBefore ) )
            return false;
        if ( !Arrays.equals( labelsAfter, other.labelsAfter ) )
            return false;
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
    
    public static final long[] EMPTY_LONG_ARRAY = new long[0];

    public static NodePropertyUpdate add( long nodeId, long propertyKeyId, Object value, long[] labels )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, null, EMPTY_LONG_ARRAY, value, labels );
    }
    
    public static NodePropertyUpdate change( long nodeId, long propertyKeyId, Object valueBefore, long[] labelsBefore,
            Object valueAfter, long[] labelsAfter )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, valueBefore, labelsBefore, valueAfter, labelsAfter );
    }
    
    public static NodePropertyUpdate remove( long nodeId, long propertyKeyId, Object value, long[] labels )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, value, labels, null, EMPTY_LONG_ARRAY );
    }
}
