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
package org.neo4j.kernel.api.index;

import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.api.index.UpdateMode;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.api.index.UpdateMode.ADDED;
import static org.neo4j.kernel.impl.api.index.UpdateMode.CHANGED;
import static org.neo4j.kernel.impl.api.index.UpdateMode.REMOVED;

public class NodePropertyUpdate
{
    private final long nodeId;
    private final int propertyKeyId;
    private final Object valueBefore;
    private final Object valueAfter;
    private final UpdateMode updateMode;
    private final long[] labelsBefore;
    private final long[] labelsAfter;

    private NodePropertyUpdate( long nodeId, int propertyKeyId, Object valueBefore, long[] labelsBefore,
                               Object valueAfter, long[] labelsAfter, UpdateMode updateMode )
    {
        this.nodeId = nodeId;
        this.propertyKeyId = propertyKeyId;
        this.valueBefore = valueBefore;
        this.labelsBefore = labelsBefore;
        this.valueAfter = valueAfter;
        this.labelsAfter = labelsAfter;
        this.updateMode = updateMode;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public int getPropertyKeyId()
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

    public int getNumberOfLabelsBefore()
    {
        return labelsBefore.length;
    }

    public int getLabelBefore( int i )
    {
        return (int) labelsBefore[i];
    }

    public int getNumberOfLabelsAfter()
    {
        return labelsAfter.length;
    }

    public int getLabelAfter( int i )
    {
        return (int) labelsAfter[i];
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
        StringBuilder result = new StringBuilder( getClass().getSimpleName() )
                .append( "[" ).append( nodeId ).append( ", prop:" ).append( propertyKeyId ).append( " " );
        switch ( updateMode )
        {
        case ADDED: result.append( "add:" ).append( valueAfter ); break;
        case CHANGED: result.append( "change:" ).append( valueBefore ).append( " => " ).append( valueAfter ); break;
        case REMOVED: result.append( "remove:" ).append( valueBefore ); break;
        default: throw new IllegalArgumentException( updateMode.toString() );
        }
        result.append( ", labelsBefore:" ).append( Arrays.toString( labelsBefore ) );
        result.append( ", labelsAfter:" ).append( Arrays.toString( labelsAfter ) );
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
        result = prime * result + propertyKeyId;
        result = prime * result + updateMode.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        NodePropertyUpdate other = (NodePropertyUpdate) obj;
        return Arrays.equals( labelsBefore, other.labelsBefore ) &&
                Arrays.equals( labelsAfter, other.labelsAfter ) &&
                nodeId == other.nodeId &&
                propertyKeyId == other.propertyKeyId &&
                updateMode == other.updateMode &&
                propertyValuesEqual( valueBefore, other.valueBefore ) &&
                propertyValuesEqual( valueAfter, other.valueAfter );
    }

    public static boolean propertyValuesEqual( Object a, Object b )
    {
        if ( a == null )
        {
            return b == null;
        }
        if ( b == null )
        {
            return false;
        }

        if (a instanceof boolean[] && b instanceof boolean[])
        {
            return Arrays.equals( (boolean[]) a, (boolean[]) b );
        }
        if (a instanceof byte[] && b instanceof byte[])
        {
            return Arrays.equals( (byte[]) a, (byte[]) b );
        }
        if (a instanceof short[] && b instanceof short[])
        {
            return Arrays.equals( (short[]) a, (short[]) b );
        }
        if (a instanceof int[] && b instanceof int[])
        {
            return Arrays.equals( (int[]) a, (int[]) b );
        }
        if (a instanceof long[] && b instanceof long[])
        {
            return Arrays.equals( (long[]) a, (long[]) b );
        }
        if (a instanceof char[] && b instanceof char[])
        {
            return Arrays.equals( (char[]) a, (char[]) b );
        }
        if (a instanceof float[] && b instanceof float[])
        {
            return Arrays.equals( (float[]) a, (float[]) b );
        }
        if (a instanceof double[] && b instanceof double[])
        {
            return Arrays.equals( (double[]) a, (double[]) b );
        }
        if (a instanceof Object[] && b instanceof Object[])
        {
            return Arrays.equals( (Object[]) a, (Object[]) b );
        }
        return a.equals( b );
    }

    public static NodePropertyUpdate add( long nodeId, int propertyKeyId, Object value, long[] labels )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, null, EMPTY_LONG_ARRAY, value, labels, ADDED );
    }

    public static NodePropertyUpdate change( long nodeId, int propertyKeyId, Object valueBefore, long[] labelsBefore,
            Object valueAfter, long[] labelsAfter )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, valueBefore, labelsBefore, valueAfter, labelsAfter,
                CHANGED );
    }

    public static NodePropertyUpdate remove( long nodeId, int propertyKeyId, Object value, long[] labels )
    {
        return new NodePropertyUpdate( nodeId, propertyKeyId, value, labels, null, EMPTY_LONG_ARRAY, REMOVED );
    }
}
