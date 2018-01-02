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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Breadth first search to find all shortest uniform paths from a node to all
 * others. I.e. assume the cost 1 for all relationships. This can be done by
 * Dijkstra with the right arguments, but this should be faster.
 * @complexity This algorithm runs in O(m) time (not including the case when m
 *             is zero).
 * @author Patrik Larsson
 */
public class SingleSourceShortestPathBFS implements
    SingleSourceShortestPath<Integer>
{
    protected Node startNode;
    protected Direction relationShipDirection;
    protected RelationshipType[] relationShipTypes;
    protected HashMap<Node,Integer> distances = new HashMap<Node,Integer>();;
    protected HashMap<Node,List<Relationship>> predecessors = new HashMap<Node,List<Relationship>>();
    // Limits
    protected long maxDepth = Long.MAX_VALUE;
    protected long depth = 0;
    LinkedList<Node> currentLayer = new LinkedList<Node>();;
    LinkedList<Node> nextLayer = new LinkedList<Node>();

    public SingleSourceShortestPathBFS( Node startNode,
        Direction relationShipDirection, RelationshipType... relationShipTypes )
    {
        super();
        this.startNode = startNode;
        this.relationShipDirection = relationShipDirection;
        this.relationShipTypes = relationShipTypes;
        reset();
    }

    /**
     * This sets the maximum depth to scan.
     */
    public void limitDepth( long maxDepth )
    {
        this.maxDepth = maxDepth;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public void setStartNode( Node node )
    {
        startNode = node;
        reset();
    }

    /**
     * @see SingleSourceShortestPath
     */
    public void reset()
    {
        distances = new HashMap<Node,Integer>();
        predecessors = new HashMap<Node,List<Relationship>>();
        currentLayer = new LinkedList<Node>();;
        nextLayer = new LinkedList<Node>();
        currentLayer.add( startNode );
        depth = 0;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public Integer getCost( Node targetNode )
    {
        calculate( targetNode );
        return distances.get( targetNode );
    }

    /**
     * @see SingleSourceShortestPath
     */
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
        return Util.constructSinglePathToNode( targetNode, predecessors, true,
            false );
    }

    /**
     * @see SingleSourceShortestPath
     */
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
        return Util.constructSinglePathToNodeAsNodes( targetNode, predecessors,
            true, false );
    }

    /**
     * @see SingleSourceShortestPath
     */
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
            predecessors, false );
    }

    /**
     * @see SingleSourceShortestPath
     */
    public List<List<PropertyContainer>> getPaths( Node targetNode )
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
        return Util.constructAllPathsToNode( targetNode, predecessors, true,
            false );
    }

    /**
     * @see SingleSourceShortestPath
     */
    public List<List<Node>> getPathsAsNodes( Node targetNode )
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
        return Util.constructAllPathsToNodeAsNodes( targetNode, predecessors,
            true, false );
    }

    /**
     * @see SingleSourceShortestPath
     */
    public List<List<Relationship>> getPathsAsRelationships( Node targetNode )
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
        return Util.constructAllPathsToNodeAsRelationships( targetNode,
            predecessors, false );
    }

    /**
     * Iterator-style "next" method.
     * @return True if evaluate was made. False if no more computation could be
     *         done.
     */
    public boolean processNextNode()
    {
        // finished with current layer? increase depth
        if ( currentLayer.isEmpty() )
        {
            if ( nextLayer.isEmpty() )
            {
                return false;
            }
            currentLayer = nextLayer;
            nextLayer = new LinkedList<Node>();
            ++depth;
        }
        Node node = currentLayer.poll();
        // Multiple paths to a certain node might make it appear several
        // times, just process it once
        if ( distances.containsKey( node ) )
        {
            return true;
        }
        // Put it in distances
        distances.put( node, (int) depth );
        // Follow all edges
        for ( RelationshipType relationshipType : relationShipTypes )
        {
            for ( Relationship relationship : node.getRelationships(
                relationshipType, relationShipDirection ) )
            {
                Node targetNode = relationship.getOtherNode( node );
                // Are we going back into the already finished area?
                // That would be more expensive.
                if ( !distances.containsKey( targetNode ) )
                {
                    // Put this into the next layer and the predecessors
                    nextLayer.add( targetNode );
                    List<Relationship> targetPreds = predecessors
                        .get( targetNode );
                    if ( targetPreds == null )
                    {
                        targetPreds = new LinkedList<Relationship>();
                        predecessors.put( targetNode, targetPreds );
                    }
                    targetPreds.add( relationship );
                }
            }
        }
        return true;
    }

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
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
        while ( depth <= maxDepth
            && (targetNode == null || !distances.containsKey( targetNode )) )
        {
            if ( !processNextNode() )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public List<Node> getPredecessorNodes( Node node )
    {
        List<Node> result = new LinkedList<Node>();
        List<Relationship> predecessorRelationShips = predecessors.get( node );
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
        calculate();
        return predecessors;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public Direction getDirection()
    {
        return relationShipDirection;
    }

    /**
     * @see SingleSourceShortestPath
     */
    public RelationshipType[] getRelationshipTypes()
    {
        return relationShipTypes;
    }
}
