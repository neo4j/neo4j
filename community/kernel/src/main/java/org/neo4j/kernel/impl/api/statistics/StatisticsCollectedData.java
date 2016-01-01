/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.statistics;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.heuristics.StatisticsData;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

public class StatisticsCollectedData implements StatisticsData, Serializable
{

    private static final long serialVersionUID = 5430534253089297623L;

    private final NodeLivenessData nodeLivenessData;
    private final LabelledDistribution<Integer> labels;
    private final LabelledDistribution<Integer> relationships;

    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> outgoingDegrees = new HashMap<>();
    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> incomingDegrees = new HashMap<>();
    private final Map</*label*/Integer, Map</*rel*/Integer, RollingAverage>> bothDegrees = new HashMap<>();
    private final RollingAverage.Parameters parameters;

    public StatisticsCollectedData()
    {
        this( new RollingAverage.Parameters() );
    }

    public StatisticsCollectedData( RollingAverage.Parameters parameters )
    {
        this.parameters = parameters;
        this.nodeLivenessData = new NodeLivenessData( parameters );
        this.labels = new LabelledDistribution<>( parameters.equalityTolerance );
        this.relationships = new LabelledDistribution<>( parameters.equalityTolerance );

        outgoingDegrees.put( RELATIONSHIP_DEGREE_FOR_NODE_WITHOUT_LABEL, new HashMap<Integer, RollingAverage>() );
        incomingDegrees.put( RELATIONSHIP_DEGREE_FOR_NODE_WITHOUT_LABEL, new HashMap<Integer, RollingAverage>() );
        bothDegrees.put( RELATIONSHIP_DEGREE_FOR_NODE_WITHOUT_LABEL, new HashMap<Integer, RollingAverage>() );
    }

    @Override
    public double labelDistribution( int labelId )
    {
        return labels.get( labelId );
    }

    @Override
    public double relationshipTypeDistribution( int relType )
    {
        return relationships.get( relType );
    }

    @Override
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

        if ( labelMap.containsKey( labelId ) )
        {
            Map<Integer, RollingAverage> relTypeMap = labelMap.get( labelId );
            if ( relTypeMap.containsKey( relType ) )
            {
                return relTypeMap.get( relType ).average();
            }
        }

        return 0.0;
    }

    @Override
    public double liveNodesRatio()
    {
        return nodeLivenessData.liveEntitiesRatio();
    }

    @Override
    public long maxAddressableNodes()
    {
        return nodeLivenessData.highestNodeId();
    }

    public void recordLabels( List<Integer> nodeLabels )
    {
        labels.record( nodeLabels );
    }

    public void recordRelationshipTypes( List<Integer> nodeRelTypes )
    {
        relationships.record( nodeRelTypes );
    }

    public void recordNodeLiveEntity()
    {
        nodeLivenessData.recordLiveEntity();
    }

    public void recordNodeDeadEntity()
    {
        nodeLivenessData.recordDeadEntity();
    }

    public void recordHighestNodeId( long nodeId )
    {
        nodeLivenessData.recordHighestId( nodeId );
    }

    public Map<Integer, Map<Integer, RollingAverage>> getBothDegree()
    {
        return bothDegrees;
    }

    public Map<Integer, Map<Integer, RollingAverage>> getIncomingDegree()
    {
        return incomingDegrees;
    }

    public Map<Integer, Map<Integer, RollingAverage>> getOutcomingDegree()
    {
        return outgoingDegrees;
    }

    public void recalculate()
    {
        labels.recalculate();
        relationships.recalculate();
        nodeLivenessData.recalculate();
    }

    public RollingAverage.Parameters getParameters()
    {
        return parameters;
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

        StatisticsCollectedData that = (StatisticsCollectedData) o;

        return bothDegrees.equals( that.bothDegrees )
                && incomingDegrees.equals( that.incomingDegrees )
                && outgoingDegrees.equals( that.outgoingDegrees )
                && nodeLivenessData.equals( that.nodeLivenessData )
                && labels.equals( that.labels )
                && relationships.equals( that.relationships );
    }

    @Override
    public int hashCode()
    {
        int result = labels.hashCode();
        result = 31 * result + relationships.hashCode();
        result = 31 * result + outgoingDegrees.hashCode();
        result = 31 * result + incomingDegrees.hashCode();
        result = 31 * result + bothDegrees.hashCode();
        result = 31 * result + nodeLivenessData.hashCode();
        return result;
    }
}
