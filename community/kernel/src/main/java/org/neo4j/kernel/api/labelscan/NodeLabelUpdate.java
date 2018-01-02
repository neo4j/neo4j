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
package org.neo4j.kernel.api.labelscan;

import java.util.Arrays;
import java.util.Comparator;

public class NodeLabelUpdate
{
    public static final Comparator<? super NodeLabelUpdate> SORT_BY_NODE_ID = new Comparator<NodeLabelUpdate>()
    {
        @Override
        public int compare( NodeLabelUpdate o1, NodeLabelUpdate o2 )
        {
            return Long.compare( o1.getNodeId(), o2.getNodeId() );
        }
    };

    private final long nodeId;
    private final long[] labelsBefore;
    private final long[] labelsAfter;

    private NodeLabelUpdate( long nodeId, long[] labelsBefore, long[] labelsAfter )
    {
        this.nodeId = nodeId;
        this.labelsBefore = labelsBefore;
        this.labelsAfter = labelsAfter;
    }

    public long getNodeId()
    {
        return nodeId;
    }

    public long[] getLabelsBefore()
    {
        return labelsBefore;
    }

    public long[] getLabelsAfter()
    {
        return labelsAfter;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[node:" + nodeId + ", labelsBefore:" + Arrays.toString( labelsBefore ) +
                ", labelsAfter:" + Arrays.toString( labelsAfter ) + "]";
    }

    public static NodeLabelUpdate labelChanges( long nodeId, long[] labelsBeforeChange, long[] labelsAfterChange )
    {
        return new NodeLabelUpdate( nodeId, labelsBeforeChange, labelsAfterChange );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        NodeLabelUpdate that = (NodeLabelUpdate) o;

        if ( nodeId != that.nodeId )
        {
            return false;
        }
        if ( !Arrays.equals( labelsAfter, that.labelsAfter ) )
        {
            return false;
        }
        if ( !Arrays.equals( labelsBefore, that.labelsBefore ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (nodeId ^ (nodeId >>> 32));
        result = 31 * result + (labelsBefore != null ? Arrays.hashCode( labelsBefore ) : 0);
        result = 31 * result + (labelsAfter != null ? Arrays.hashCode( labelsAfter ) : 0);
        return result;
    }
}
