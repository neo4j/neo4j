/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

public class HeuristicsData implements Serializable
{
    public static final int WINDOW_SIZE = 1024;

    private static final long serialVersionUID = 5430534253089297623L;

    private final LabelledDistribution<Integer> labels = new LabelledDistribution<>();
    private final LabelledDistribution<Integer> relationships = new LabelledDistribution<>();

    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> outgoingDegrees = new HashMap<>();
    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> incomingDegrees = new HashMap<>();
    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> bothDegrees = new HashMap<>();

    private final EntityLivenessData nodeLivenessData = new EntityLivenessData();

    public HeuristicsData()
    {
        outgoingDegrees.put( -1, new HashMap<Integer, RollingAverage>() );
        incomingDegrees.put( -1, new HashMap<Integer, RollingAverage>() );
        bothDegrees.put(     -1, new HashMap<Integer, RollingAverage>() );
    }

    public void addNodeObservation( List<Integer> nodeLabels, List<Integer> nodeRelTypes,
                                    Map<Integer, Integer> nodeIncoming, Map<Integer, Integer> nodeOutgoing )
    {
        labels.record( nodeLabels );
        relationships.record( nodeRelTypes );

        recordNodeDegree( nodeLabels, nodeIncoming, incomingDegrees );
        recordNodeDegree( nodeLabels, nodeOutgoing, outgoingDegrees );

        recordNodeDegree( nodeLabels, nodeIncoming, bothDegrees );
        recordNodeDegree( nodeLabels, nodeOutgoing, bothDegrees );

        nodeLivenessData.recordLiveEntity();
    }

    public void addSkippedNodeObservation()
    {
        nodeLivenessData.recordDeadEntity();
    }

    public void addMaxNodesObservation( long maxNodes ) { nodeLivenessData.setMaxEntities(maxNodes); }

    private void recordNodeDegree( List<Integer> nodeLabels,
                                   Map<Integer, Integer> source,
                                   Map<Integer, Map<Integer, RollingAverage>> degreeMap )
    {
        for ( Map.Entry<Integer, Integer> entry : source.entrySet() )
        {
            for ( Integer nodeLabel : Iterables.append( /* Include for looking up without label */-1, nodeLabels) )
            {
                Map<Integer, RollingAverage> reltypeMap = degreeMap.get( nodeLabel );
                if(reltypeMap == null)
                {
                    reltypeMap = new HashMap<>();
                    degreeMap.put( nodeLabel, reltypeMap );
                }

                RollingAverage histogram = reltypeMap.get( entry.getKey() );
                if(histogram == null)
                {
                    histogram = new RollingAverage( WINDOW_SIZE );
                    reltypeMap.put( entry.getKey(), histogram );
                }

                histogram.record( entry.getValue() );
            }
        }
    }

    public void recalculate()
    {
        labels.recalculate();
        relationships.recalculate();
        nodeLivenessData.recalculate();
    }

    public LabelledDistribution<Integer> labels()
    {
        return labels;
    }

    public LabelledDistribution<Integer> relationships()
    {
        return relationships;
    }

    public double degree( int labelId, int relType, Direction direction )
    {
        Map<Integer, Map<Integer, RollingAverage>> labelMap;
        switch ( direction )
        {
            case INCOMING:
                labelMap = incomingDegrees;
                break;
            case OUTGOING:
                labelMap = outgoingDegrees;
                break;
            default:
                labelMap = bothDegrees;
        }

        if(labelMap.containsKey( labelId ))
        {
            Map<Integer, RollingAverage> relTypeMap = labelMap.get( labelId );
            if(relTypeMap.containsKey( relType ))
            {
                return relTypeMap.get( relType ).average();
            }
        }

        return 0.0;
    }

    public double liveNodesRatio()
    {
        return nodeLivenessData.liveEntitiesRatio();
    }

    public long maxAddressableNodes()
    {
        return nodeLivenessData.maxAddressableEntities();
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

        HeuristicsData that = (HeuristicsData) o;

        if ( !labels.equals( that.labels, 0.0001f ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return labels.hashCode();
    }
}
