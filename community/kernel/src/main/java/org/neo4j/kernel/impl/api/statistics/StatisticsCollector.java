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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.heuristics.StatisticsData;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.util.statistics.RollingAverage;

import static org.neo4j.helpers.collection.IteratorUtil.asList;

public class StatisticsCollector implements Runnable
{

    private final StatisticsCollectedData collectedData;
    private final StoreReadLayer store;
    private final Random random = new Random();

    public StatisticsCollector( StoreReadLayer store, StatisticsCollectedData collectedData )
    {
        this.collectedData = collectedData;
        this.store = store;
    }

    private void addNodeObservation( List<Integer> nodeLabels, List<Integer> nodeRelTypes,
                                     Map<Integer, Integer> nodeIncoming, Map<Integer, Integer> nodeOutgoing )
    {
        collectedData.recordLabels( nodeLabels );
        collectedData.recordRelationshipTypes( nodeRelTypes );

        recordNodeDegree( nodeLabels, nodeIncoming, collectedData.getIncomingDegree() );
        recordNodeDegree( nodeLabels, nodeOutgoing, collectedData.getOutcomingDegree() );

        recordNodeDegree( nodeLabels, nodeIncoming, collectedData.getBothDegree() );
        recordNodeDegree( nodeLabels, nodeOutgoing, collectedData.getBothDegree() );

        collectedData.recordNodeLiveEntity();
    }

    private void recordNodeDegree( List<Integer> nodeLabels,
                                   Map<Integer, Integer> source,
                                   Map<Integer, Map<Integer, RollingAverage>> degreeMap )
    {
        for ( Map.Entry<Integer, Integer> entry : source.entrySet() )
        {
            for ( Integer nodeLabel :
                    Iterables.append( /* Include for looking up without label */
                            StatisticsData.RELATIONSHIP_DEGREE_FOR_NODE_WITHOUT_LABEL,
                            nodeLabels
                    )
                    )
            {
                Map<Integer, RollingAverage> reltypeMap = degreeMap.get( nodeLabel );
                if ( reltypeMap == null )
                {
                    reltypeMap = new HashMap<>();
                    degreeMap.put( nodeLabel, reltypeMap );
                }

                RollingAverage histogram = reltypeMap.get( entry.getKey() );
                if ( histogram == null )
                {
                    histogram = new RollingAverage( collectedData.getParameters() );
                    reltypeMap.put( entry.getKey(), histogram );
                }

                histogram.record( entry.getValue() );
            }
        }
    }

    public StatisticsData collectedData()
    {
        return collectedData;
    }

    /**
     * Perform one sampling run.
     */
    @Override
    public void run()
    {
        for ( int i = 0; i < 100; i++ )
        {
            long id = random.nextLong() % store.highestNodeIdInUse();
            if ( store.nodeExists( id ) )
            {
                try
                {
                    List<Integer> relTypes = asList( store.nodeGetRelationshipTypes( id ) );
                    List<Integer> labels = asList( store.nodeGetLabels( id ) );

                    Map<Integer, Integer> incomingDegrees = new HashMap<>();
                    Map<Integer, Integer> outgoingDegrees = new HashMap<>();

                    for ( Integer relType : relTypes )
                    {
                        incomingDegrees.put( relType, store.nodeGetDegree( id, Direction.INCOMING, relType ) );
                        outgoingDegrees.put( relType, store.nodeGetDegree( id, Direction.OUTGOING, relType ) );
                    }

                    addNodeObservation( labels, relTypes, incomingDegrees, outgoingDegrees );
                }
                catch ( EntityNotFoundException e )
                {
                    // Node was deleted while we read it, or something. In any case, just exclude it from the run.
                    collectedData.recordNodeDeadEntity();
                }
            }
            else
            {
                collectedData.recordNodeDeadEntity();
            }
        }

        collectedData.recordHighestNodeId( store.highestNodeIdInUse() );

        collectedData.recalculate();
    }
}

