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
package org.neo4j.graphalgo.impl.shortestpath;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Dijkstra implementation to solve the single source shortest path problem for
 * weighted networks.
 * @complexity The {@link CostEvaluator}, the {@link CostAccumulator} and the
 *             cost comparator will all be called once for every relationship
 *             traversed. Assuming they run in constant time, the time
 *             complexity for this algorithm is O(m + n * log(n)).
 * @author Patrik Larsson
 * @param <CostType>
 *            The datatype the edge weights are represented by.
 */
public class SingleSourceShortestPathDijkstra<CostType> extends
    Dijkstra<CostType> implements SingleSourceShortestPath<CostType>
{
    DijstraIterator dijstraIterator;

    /**
     * @see Dijkstra
     */
    public SingleSourceShortestPathDijkstra( CostType startCost,
        Node startNode, CostEvaluator<CostType> costEvaluator,
        CostAccumulator<CostType> costAccumulator,
        Comparator<CostType> costComparator, Direction relationDirection,
        RelationshipType... costRelationTypes )
    {
        super( startCost, startNode, null, costEvaluator, costAccumulator,
            costComparator, relationDirection, costRelationTypes );
        reset();
    }

    protected HashMap<Node,CostType> distances = new HashMap<Node,CostType>();

    @Override
    public void reset()
    {
        super.reset();
        distances = new HashMap<Node,CostType>();
        HashMap<Node,CostType> seen1 = new HashMap<Node,CostType>();
        HashMap<Node,CostType> seen2 = new HashMap<Node,CostType>();
        HashMap<Node,CostType> dists2 = new HashMap<Node,CostType>();
        dijstraIterator = new DijstraIterator( startNode, predecessors1, seen1,
            seen2, distances, dists2, false );
    }

    /**
     * Same as calculate(), but will set the flag to calculate all shortest
     * paths. It sets the flag and then calls calculate.
     * @return
     */
    public boolean calculateMultiple( Node targetNode )
    {
        if ( !calculateAllShortestPaths )
        {
            reset();
            calculateAllShortestPaths = true;
        }
        return calculate( targetNode );
    }

    @Override
    public boolean calculate()
    {
        return calculate( null );
    }

    /**
     * Internal calculate method that will run the calculation until either the
     * limit is reached or a result has been generated for a given node.
     */
    public boolean calculate( Node targetNode )
    {
        while ( (targetNode == null || !distances.containsKey( targetNode ))
            && dijstraIterator.hasNext() && !limitReached() )
        {
            dijstraIterator.next();
        }
        return true;
    }

    // We dont need to reset the calculation, so we just override this.
    @Override
    public void setEndNode( Node endNode )
    {
        this.endNode = endNode;
    }

    /**
     * @see Dijkstra
     */
    public CostType getCost( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculate( targetNode );
        return distances.get( targetNode );
    }

    public List<List<PropertyContainer>> getPaths( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculateMultiple( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return new LinkedList<List<PropertyContainer>>( Util
            .constructAllPathsToNode( targetNode, predecessors1, true, false ) );
    }

    public List<List<Node>> getPathsAsNodes( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculateMultiple( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return new LinkedList<List<Node>>( Util.constructAllPathsToNodeAsNodes(
            targetNode, predecessors1, true, false ) );
    }

    public List<List<Relationship>> getPathsAsRelationships( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculateMultiple( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return new LinkedList<List<Relationship>>( Util
            .constructAllPathsToNodeAsRelationships( targetNode, predecessors1,
                false ) );
    }

    public List<PropertyContainer> getPath( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculate( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return Util.constructSinglePathToNode( targetNode, predecessors1, true,
            false );
    }

    public List<Node> getPathAsNodes( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculate( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return Util.constructSinglePathToNodeAsNodes( targetNode,
            predecessors1, true, false );
    }

    public List<Relationship> getPathAsRelationships( Node targetNode )
    {
        if ( targetNode == null )
        {
            throw new RuntimeException( "No end node defined" );
        }
        calculate( targetNode );
        if ( !distances.containsKey( targetNode ) )
        {
            return null;
        }
        return Util.constructSinglePathToNodeAsRelationships( targetNode,
            predecessors1, false );
    }

    // Override all the result-getters
    @Override
    public CostType getCost()
    {
        return getCost( endNode );
    }

    @Override
    public List<PropertyContainer> getPath()
    {
        return getPath( endNode );
    }

    @Override
    public List<Node> getPathAsNodes()
    {
        return getPathAsNodes( endNode );
    }

    @Override
    public List<Relationship> getPathAsRelationships()
    {
        return getPathAsRelationships( endNode );
    }

    @Override
    public List<List<PropertyContainer>> getPaths()
    {
        return getPaths( endNode );
    }

    @Override
    public List<List<Node>> getPathsAsNodes()
    {
        return getPathsAsNodes( endNode );
    }

    @Override
    public List<List<Relationship>> getPathsAsRelationships()
    {
        return getPathsAsRelationships( endNode );
    }

    /**
     * @see SingleSourceShortestPath
     */
    public List<Node> getPredecessorNodes( Node node )
    {
        List<Node> result = new LinkedList<Node>();
        List<Relationship> predecessorRelationShips = predecessors1.get( node );
        if ( predecessorRelationShips == null
            || predecessorRelationShips.size() == 0 )
        {
            return null;
        }
        for ( Relationship relationship : predecessorRelationShips )
        {
            result.add( relationship.getOtherNode( node ) );
        }
        return result;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public Map<Node,List<Relationship>> getPredecessors()
    {
        calculateMultiple();
        return predecessors1;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public Direction getDirection()
    {
        return relationDirection;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public RelationshipType[] getRelationshipTypes()
    {
        return costRelationTypes;
    }
}
