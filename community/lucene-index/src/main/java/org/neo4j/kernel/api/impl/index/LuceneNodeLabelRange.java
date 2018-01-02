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
package org.neo4j.kernel.api.impl.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.direct.NodeLabelRange;

public class LuceneNodeLabelRange implements NodeLabelRange
{
    private final int id;
    private final long[] nodeIds;
    private final long[][] labelIds;

    public LuceneNodeLabelRange( int id, long[] nodeIds, long[][] labelIds )
    {
        this.id = id;
        this.labelIds = labelIds;
        this.nodeIds = nodeIds;
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder( "NodeLabelRange[docId=" ).append( id );
        result.append( "; {" );
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            if ( i != 0 )
            {
                result.append( ", " );
            }
            result.append( "Node[" ).append( nodeIds[i] ).append( "]: Labels[" );
            String sep = "";
            for ( long labelId : labelIds[i] )
            {
                result.append( sep ).append( labelId );
                sep = ", ";
            }
            result.append( "]" );
        }
        return result.append( "}]" ).toString();
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public long[] nodes()
    {
        return nodeIds;
    }

    @Override
    public long[] labels( long nodeId )
    {
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            if ( nodeId == nodeIds[i] )
            {
                return labelIds[i];
            }
        }
        throw new IllegalArgumentException( "Unknown nodeId: " + nodeId );
    }

    public static LuceneNodeLabelRange fromBitmapStructure( int id, long[] labelIds, long[][] nodeIdsByLabelIndex )
    {
        Map<Long, List<Long>> labelsForEachNode = new HashMap<>(  );
        for ( int i = 0; i < labelIds.length; i++ )
        {
            long labelId = labelIds[i];
            for ( int j = 0; j < nodeIdsByLabelIndex[i].length; j++ )
            {
                long nodeId = nodeIdsByLabelIndex[i][j];
                List<Long> labelIdList = labelsForEachNode.get( nodeId );
                if ( labelIdList == null )
                {
                    labelsForEachNode.put( nodeId, labelIdList = new ArrayList<>() );
                }
                labelIdList.add( labelId );
            }
        }

        Set<Long> nodeIdSet = labelsForEachNode.keySet();
        long[] nodeIds = new long[ nodeIdSet.size() ];
        long[][] labelIdsByNodeIndex = new long[ nodeIdSet.size() ][];
        int nodeIndex = 0;
        for ( long nodeId : nodeIdSet )
        {
            nodeIds[ nodeIndex ] = nodeId;
            List<Long> labelIdList = labelsForEachNode.get( nodeId );
            long[] nodeLabelIds = new long[ labelIdList.size() ];
            int labelIndex = 0;
            for ( long labelId : labelIdList )
            {
                nodeLabelIds[ labelIndex ] = labelId;
                labelIndex++;
            }
            labelIdsByNodeIndex[ nodeIndex ] = nodeLabelIds;
            nodeIndex++;
        }
        return new LuceneNodeLabelRange( id, nodeIds, labelIdsByNodeIndex );
    }
}
