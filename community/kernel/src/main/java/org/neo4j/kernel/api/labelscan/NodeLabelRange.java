/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongList;

import static java.lang.Math.toIntExact;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * Represents a range of nodes and label ids attached to those nodes. All nodes in the range are present in
 * {@link #nodes() nodes array}, but not all node ids will have corresponding {@link #labels(long) labels},
 * where an empty long[] will be returned instead.
 */
public class NodeLabelRange
{
    private final long idRange;
    private final long[] nodes;
    private final long[][] labels;

    /**
     * @param idRange node id range, e.g. in which id span the nodes are.
     * @param labels long[][] where first dimension is relative node id in this range, i.e. 0-rangeSize
     * and second the label ids for that node, potentially empty if there are none for that node.
     * The first dimension must be the size of the range.
     */
    public NodeLabelRange( long idRange, long[][] labels )
    {
        this.idRange = idRange;
        this.labels = labels;
        int rangeSize = labels.length;
        long baseNodeId = idRange * rangeSize;

        this.nodes = new long[rangeSize];
        for ( int i = 0; i < rangeSize; i++ )
        {
            nodes[i] = baseNodeId + i;
        }
    }

    /**
     * @return the range id of this range. This is the base node id divided by range size.
     * Example: A store with nodes 1,3,20,22 and a range size of 16 would return ranges:
     * - rangeId=0, nodes=1,3
     * - rangeId=1, nodes=20,22
     */
    public long id()
    {
        return idRange;
    }

    /**
     * @return node ids in this range, the nodes in this array may or may not have {@link #labels(long) labels}
     * attached to it.
     */
    public long[] nodes()
    {
        return nodes;
    }

    /**
     * Returns the label ids (as longs) for the given node id. The {@code nodeId} must be one of the ids
     * from {@link #nodes()}.
     *
     * @param nodeId the node id to return labels for.
     * @return label ids for the given {@code nodeId}.
     */
    public long[] labels( long nodeId )
    {
        long firstNodeId = idRange * labels.length;
        int index = toIntExact( nodeId - firstNodeId );
        assert index >= 0 && index < labels.length : "nodeId:" + nodeId + ", idRange:" + idRange;
        return labels[index] != null ? labels[index] : EMPTY_LONG_ARRAY;
    }

    private static String toString( String prefix, long[] nodes, long[][] labels )
    {
        StringBuilder result = new StringBuilder( prefix );
        result.append( "; {" );
        for ( int i = 0; i < nodes.length; i++ )
        {
            if ( i != 0 )
            {
                result.append( ", " );
            }
            result.append( "Node[" ).append( nodes[i] ).append( "]: Labels[" );
            String sep = "";
            if ( labels[i] != null )
            {
                for ( long labelId : labels[i] )
                {
                    result.append( sep ).append( labelId );
                    sep = ", ";
                }
            }
            else
            {
                result.append( "null" );
            }
            result.append( ']' );
        }
        return result.append( "}]" ).toString();
    }

    @Override
    public String toString()
    {
        String rangeString = idRange * labels.length + "-" + (idRange + 1) * labels.length;
        String prefix = "NodeLabelRange[idRange=" + rangeString;
        return toString( prefix, nodes, labels );
    }

    public static void readBitmap( long bitmap, long labelId, PrimitiveLongList[] labelsPerNode )
    {
        while ( bitmap != 0 )
        {
            int relativeNodeId = Long.numberOfTrailingZeros( bitmap );
            if ( labelsPerNode[relativeNodeId] == null )
            {
                labelsPerNode[relativeNodeId] = Primitive.longList();
            }
            labelsPerNode[relativeNodeId].add( labelId );
            bitmap &= bitmap - 1;
        }
    }

    public static long[][] convertState( PrimitiveLongList[] state )
    {
        long[][] labelIdsByNodeIndex = new long[state.length][];
        for ( int i = 0; i < state.length; i++ )
        {
            PrimitiveLongList labelIdList = state[i];
            if ( labelIdList != null )
            {
                labelIdsByNodeIndex[i] = labelIdList.toArray();
            }
        }
        return labelIdsByNodeIndex;
    }
}
