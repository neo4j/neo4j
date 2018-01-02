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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Dijkstra class. This class can be used to perform shortest path computations
 * between two nodes. The search is made simultaneously from both the start node
 * and the end node. Note that per default, only one shortest path will be
 * searched for. This will be done when the path or the cost is asked for. If at
 * some later time getPaths is called to get all the paths, the calculation is
 * redone. In order to avoid this double computation when all paths are desired,
 * be sure to call getPaths (or calculateMultiple) before any call to getPath or
 * getCost (or calculate) is made.
 *
 * @complexity The {@link CostEvaluator}, the {@link CostAccumulator} and the
 *             cost comparator will all be called once for every relationship
 *             traversed. Assuming they run in constant time, the time
 *             complexity for this algorithm is O(m + n * log(n)).
 * @author Patrik Larsson
 * @param <CostType> The datatype the edge weights will be represented by.
 */
public class Dijkstra<CostType> implements
        SingleSourceSingleSinkShortestPath<CostType>
{
    protected CostType startCost; // starting cost for both the start node and
    // the end node
    protected Node startNode, endNode;
    protected RelationshipType[] costRelationTypes;
    protected Direction relationDirection;
    protected CostEvaluator<CostType> costEvaluator = null;
    protected CostAccumulator<CostType> costAccumulator = null;
    protected Comparator<CostType> costComparator = null;
    protected boolean calculateAllShortestPaths = false;
    // Limits
    protected long maxRelationShipsToTraverse = -1;
    protected long numberOfTraversedRelationShips = 0;
    protected long maxNodesToTraverse = -1;
    protected long numberOfNodesTraversed = 0;
    protected CostType maxCost = null;

    /**
     * @return True if the set limits for the calculation has been reached (but
     *         not exceeded)
     */
    protected boolean limitReached()
    {
        if ( maxRelationShipsToTraverse >= 0
             && numberOfTraversedRelationShips >= maxRelationShipsToTraverse )
        {
            return true;
        }
        if ( maxNodesToTraverse >= 0
             && numberOfNodesTraversed >= maxNodesToTraverse )
        {
            return true;
        }
        return false;
    }

    protected boolean limitReached( CostType cost1, CostType cost2 )
    {
        if ( maxCost != null )
        {
            CostType totalCost = costAccumulator.addCosts( cost1, cost2 );
            if ( costComparator.compare( totalCost, maxCost ) > 0 )
            {
                foundPathsMiddleNodes = null;
                foundPathsCost = null;
                return true;
            }
        }

        return false;
    }

    // Result data
    protected boolean doneCalculation = false;
    protected Set<Node> foundPathsMiddleNodes = null;
    protected CostType foundPathsCost;
    protected HashMap<Node, List<Relationship>> predecessors1 = new HashMap<Node, List<Relationship>>();
    protected HashMap<Node, List<Relationship>> predecessors2 = new HashMap<Node, List<Relationship>>();

    /**
     * Resets the result data to force the computation to be run again when some
     * result is asked for.
     */
    public void reset()
    {
        doneCalculation = false;
        foundPathsMiddleNodes = null;
        predecessors1 = new HashMap<Node, List<Relationship>>();
        predecessors2 = new HashMap<Node, List<Relationship>>();
        // Limits
        numberOfTraversedRelationShips = 0;
        numberOfNodesTraversed = 0;
    }

    /**
     * @param startCost Starting cost for both the start node and the end node
     * @param startNode the start node
     * @param endNode the end node
     * @param costRelationTypes the relationship that should be included in the
     *            path
     * @param relationDirection relationship direction to follow
     * @param costEvaluator the cost function per relationship
     * @param costAccumulator adding up the path cost
     * @param costComparator comparing to path costs
     */
    public Dijkstra( CostType startCost, Node startNode, Node endNode,
            CostEvaluator<CostType> costEvaluator,
            CostAccumulator<CostType> costAccumulator,
            Comparator<CostType> costComparator, Direction relationDirection,
            RelationshipType... costRelationTypes )
    {
        super();
        this.startCost = startCost;
        this.startNode = startNode;
        this.endNode = endNode;
        this.costRelationTypes = costRelationTypes;
        this.relationDirection = relationDirection;
        this.costEvaluator = costEvaluator;
        this.costAccumulator = costAccumulator;
        this.costComparator = costComparator;
    }

    /**
     * A DijkstraIterator computes the distances to nodes from a specified
     * starting node, one at a time, following the dijkstra algorithm.
     *
     * @author Patrik Larsson
     */
    protected class DijstraIterator implements Iterator<Node>
    {
        protected Node startNode;
        // where do we come from
        protected HashMap<Node, List<Relationship>> predecessors;
        // observed distances not yet final
        protected HashMap<Node, CostType> mySeen;
        protected HashMap<Node, CostType> otherSeen;
        // the final distances
        protected HashMap<Node, CostType> myDistances;
        protected HashMap<Node, CostType> otherDistances;
        // Flag that indicates if we should follow egdes in the opposite
        // direction instead
        protected boolean backwards = false;
        // The priority queue
        protected DijkstraPriorityQueue<CostType> queue;
        // "Done" flags. The first is set to true when a node is found that is
        // contained in both myDistances and otherDistances. This means the
        // calculation has found one of the shortest paths.
        protected boolean oneShortestPathHasBeenFound = false;
        protected boolean allShortestPathsHasBeenFound = false;

        public DijstraIterator( Node startNode,
                HashMap<Node, List<Relationship>> predecessors,
                HashMap<Node, CostType> mySeen,
                HashMap<Node, CostType> otherSeen,
                HashMap<Node, CostType> myDistances,
                HashMap<Node, CostType> otherDistances, boolean backwards )
        {
            super();
            this.startNode = startNode;
            this.predecessors = predecessors;
            this.mySeen = mySeen;
            this.otherSeen = otherSeen;
            this.myDistances = myDistances;
            this.otherDistances = otherDistances;
            this.backwards = backwards;
            InitQueue();
        }

        /**
         * @return The direction to use when searching for relations/edges
         */
        protected Direction getDirection()
        {
            if ( backwards )
            {
                if ( relationDirection.equals( Direction.INCOMING ) )
                {
                    return Direction.OUTGOING;
                }
                if ( relationDirection.equals( Direction.OUTGOING ) )
                {
                    return Direction.INCOMING;
                }
            }
            return relationDirection;
        }

        // This puts the start node into the queue
        protected void InitQueue()
        {
            queue = new DijkstraPriorityQueueFibonacciImpl<CostType>(
                    costComparator );
            queue.insertValue( startNode, startCost );
            mySeen.put( startNode, startCost );
        }

        public boolean hasNext()
        {
            return !queue.isEmpty() && !limitReached();
        }

        public void remove()
        {
            // Not used
            // Could be used to generate more sollutions, by removing an edge
            // from the sollution and run again?
        }

        /**
         * This checks if a node has been seen by the other iterator/traverser
         * as well. In that case a path has been found. In that case, the total
         * cost for the path is calculated and compared to previously found
         * paths.
         *
         * @param currentNode The node to be examined.
         * @param currentCost The cost from the start node to this node.
         * @param otherSideDistances Map over distances from other side. A path
         *            is found and examined if this contains currentNode.
         */
        protected void checkForPath( Node currentNode, CostType currentCost,
                HashMap<Node, CostType> otherSideDistances )
        {
            // Found a path?
            if ( otherSideDistances.containsKey( currentNode ) )
            {
                // Is it better than previously found paths?
                CostType otherCost = otherSideDistances.get( currentNode );
                CostType newTotalCost = costAccumulator.addCosts( currentCost,
                        otherCost );
                if ( foundPathsMiddleNodes == null )
                {
                    foundPathsMiddleNodes = new HashSet<Node>();
                }
                // No previous path found, or equally good one found?
                if ( foundPathsMiddleNodes.size() == 0
                     || costComparator.compare( foundPathsCost, newTotalCost ) == 0 )
                {
                    foundPathsCost = newTotalCost; // in case we had no
                    // previous path
                    foundPathsMiddleNodes.add( currentNode );
                }
                // New better path found?
                else if ( costComparator.compare( foundPathsCost, newTotalCost ) > 0 )
                {
                    foundPathsMiddleNodes.clear();
                    foundPathsCost = newTotalCost;
                    foundPathsMiddleNodes.add( currentNode );
                }
            }
        }

        public Node next()
        {
            Node currentNode = queue.extractMin();
            CostType currentCost = mySeen.get( currentNode );
            // Already done with this node?
            if ( myDistances.containsKey( currentNode ) )
            {
                return null;
            }
            if ( limitReached() )
            {
                return null;
            }
            ++numberOfNodesTraversed;
            myDistances.put( currentNode, currentCost );
            // TODO: remove from seen or not? probably not... because of path
            // detection
            // Check if we have found a better path
            checkForPath( currentNode, currentCost, otherSeen );
            // Found a path? (abort traversing from this node)
            if ( otherDistances.containsKey( currentNode ) )
            {
                oneShortestPathHasBeenFound = true;
            }
            else
            {
                // Otherwise, follow all edges from this node
                for ( RelationshipType costRelationType : costRelationTypes )
                {
                    for ( Relationship relationship : currentNode.getRelationships(
                            costRelationType, getDirection() ) )
                    {
                        if ( limitReached() )
                        {
                            break;
                        }
                        ++numberOfTraversedRelationShips;
                        // Target node
                        Node target = relationship.getOtherNode( currentNode );
                        // Find out if an eventual path would go in the opposite
                        // direction of the edge
                        boolean backwardsEdge = relationship.getEndNode().equals(
                                currentNode )
                                                ^ backwards;
                        CostType newCost = costAccumulator.addCosts(
                                currentCost, costEvaluator.getCost(
                                        relationship,
                                        backwardsEdge ? Direction.INCOMING
                                                : Direction.OUTGOING ) );
                        // Already done with target node?
                        if ( myDistances.containsKey( target ) )
                        {
                            // Have we found a better cost for a node which is
                            // already
                            // calculated?
                            if ( costComparator.compare(
                                    myDistances.get( target ), newCost ) > 0 )
                            {
                                throw new RuntimeException(
                                        "Cycle with negative costs found." );
                            }
                            // Equally good path found?
                            else if ( calculateAllShortestPaths
                                      && costComparator.compare(
                                              myDistances.get( target ),
                                              newCost ) == 0 )
                            {
                                // Put it in predecessors
                                List<Relationship> myPredecessors = predecessors.get( currentNode );
                                // Dont do it if this relation is already in
                                // predecessors (other direction)
                                if ( myPredecessors == null
                                     || !myPredecessors.contains( relationship ) )
                                {
                                    List<Relationship> predList = predecessors.get( target );
                                    if ( predList == null )
                                    {
                                        // This only happens if we get back to
                                        // the
                                        // start node, which is just bogus
                                    }
                                    else
                                    {
                                        predList.add( relationship );
                                    }
                                }
                            }
                            continue;
                        }
                        // Have we found a better cost for this node?
                        if ( !mySeen.containsKey( target )
                             || costComparator.compare( mySeen.get( target ),
                                     newCost ) > 0 )
                        {
                            // Put it in the queue
                            if ( !mySeen.containsKey( target ) )
                            {
                                queue.insertValue( target, newCost );
                            }
                            // or update the entry. (It is important to keep
                            // these
                            // cases apart to limit the size of the queue)
                            else
                            {
                                queue.decreaseValue( target, newCost );
                            }
                            // Update it
                            mySeen.put( target, newCost );
                            // Put it in predecessors
                            List<Relationship> predList = new LinkedList<Relationship>();
                            predList.add( relationship );
                            predecessors.put( target, predList );
                        }
                        // Have we found an equal cost for (additonal path to)
                        // this
                        // node?
                        else if ( calculateAllShortestPaths
                                  && costComparator.compare(
                                          mySeen.get( target ), newCost ) == 0 )
                        {
                            // Put it in predecessors
                            List<Relationship> predList = predecessors.get( target );
                            predList.add( relationship );
                        }
                    }
                }
            }
            // Check how far we need to continue when searching for all shortest
            // paths
            if ( calculateAllShortestPaths && oneShortestPathHasBeenFound )
            {
                // If we cannot continue or continuation would only find more
                // expensive paths: conclude that all shortest paths have been
                // found.
                allShortestPathsHasBeenFound = queue.isEmpty()
                                               || costComparator.compare(
                                                       mySeen.get( queue.peek() ),
                                                       currentCost ) > 0;
            }
            return currentNode;
        }

        public boolean isDone()
        {
            if ( !calculateAllShortestPaths )
            {
                return oneShortestPathHasBeenFound;
            }
            return allShortestPathsHasBeenFound;
        }
    }

    /**
     * Same as calculate(), but will set the flag to calculate all shortest
     * paths. It sets the flag and then calls calculate, so inheriting classes
     * only need to override calculate().
     *
     * @return
     */
    public boolean calculateMultiple()
    {
        if ( !calculateAllShortestPaths )
        {
            reset();
            calculateAllShortestPaths = true;
        }
        return calculate();
    }

    /**
     * Makes the main calculation If some limit is set, the shortest path(s)
     * that could be found within those limits will be calculated.
     *
     * @return True if a path was found.
     */
    public boolean calculate()
    {
        // Do this first as a general error check since this is supposed to be
        // called whenever a result is asked for.
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        // Don't do it more than once
        if ( doneCalculation )
        {
            return true;
        }
        doneCalculation = true;
        // Special case when path length is zero
        if ( startNode.equals( endNode ) )
        {
            foundPathsMiddleNodes = new HashSet<Node>();
            foundPathsMiddleNodes.add( startNode );
            foundPathsCost = costAccumulator.addCosts( startCost, startCost );
            return true;
        }
        HashMap<Node, CostType> seen1 = new HashMap<Node, CostType>();
        HashMap<Node, CostType> seen2 = new HashMap<Node, CostType>();
        HashMap<Node, CostType> dists1 = new HashMap<Node, CostType>();
        HashMap<Node, CostType> dists2 = new HashMap<Node, CostType>();
        DijstraIterator iter1 = new DijstraIterator( startNode, predecessors1,
                seen1, seen2, dists1, dists2, false );
        DijstraIterator iter2 = new DijstraIterator( endNode, predecessors2,
                seen2, seen1, dists2, dists1, true );
        Node node1 = null;
        Node node2 = null;
        while ( iter1.hasNext() && iter2.hasNext() )
        {
            if ( limitReached() )
            {
                break;
            }
            if ( iter1.hasNext() )
            {
                node1 = iter1.next();
                if ( node1 == null )
                {
                    break;
                }
            }
            if ( limitReached() )
            {
                break;
            }
            if ( !iter1.isDone() && iter2.hasNext() )
            {
                node2 = iter2.next();
                if ( node2 == null )
                {
                    break;
                }
            }
            if ( limitReached( seen1.get( node1 ), seen2.get( node2 ) ) )
            {
                break;
            }
            if ( iter1.isDone() || iter2.isDone() ) // A path was found
            {
                return true;
            }
        }

        return false;
    }

    /**
     * @return The cost for the found path(s).
     */
    public CostType getCost()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculate();
        return foundPathsCost;
    }

    /**
     * @return All the found paths or null.
     */
    public List<List<PropertyContainer>> getPaths()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculateMultiple();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return Collections.emptyList();
        }
        // Currently we use a set to avoid duplicate paths
        // TODO: can this be done smarter?
        Set<List<PropertyContainer>> paths = new HashSet<List<PropertyContainer>>();
        for ( Node middleNode : foundPathsMiddleNodes )
        {
            List<List<PropertyContainer>> paths1 = Util.constructAllPathsToNode(
                    middleNode, predecessors1, true, false );
            List<List<PropertyContainer>> paths2 = Util.constructAllPathsToNode(
                    middleNode, predecessors2, false, true );
            // For all combinations...
            for ( List<PropertyContainer> part1 : paths1 )
            {
                for ( List<PropertyContainer> part2 : paths2 )
                {
                    // Combine them
                    LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
                    path.addAll( part1 );
                    path.addAll( part2 );
                    // Add to collection
                    paths.add( path );
                }
            }
        }
        return new LinkedList<List<PropertyContainer>>( paths );
    }

    /**
     * @return All the found paths or null.
     */
    public List<List<Node>> getPathsAsNodes()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculateMultiple();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return null;
        }
        // Currently we use a set to avoid duplicate paths
        // TODO: can this be done smarter?
        Set<List<Node>> paths = new HashSet<List<Node>>();
        for ( Node middleNode : foundPathsMiddleNodes )
        {
            List<List<Node>> paths1 = Util.constructAllPathsToNodeAsNodes(
                    middleNode, predecessors1, true, false );
            List<List<Node>> paths2 = Util.constructAllPathsToNodeAsNodes(
                    middleNode, predecessors2, false, true );
            // For all combinations...
            for ( List<Node> part1 : paths1 )
            {
                for ( List<Node> part2 : paths2 )
                {
                    // Combine them
                    LinkedList<Node> path = new LinkedList<Node>();
                    path.addAll( part1 );
                    path.addAll( part2 );
                    // Add to collection
                    paths.add( path );
                }
            }
        }
        return new LinkedList<List<Node>>( paths );
    }

    /**
     * @return All the found paths or null.
     */
    public List<List<Relationship>> getPathsAsRelationships()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculateMultiple();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return null;
        }
        // Currently we use a set to avoid duplicate paths
        // TODO: can this be done smarter?
        Set<List<Relationship>> paths = new HashSet<List<Relationship>>();
        for ( Node middleNode : foundPathsMiddleNodes )
        {
            List<List<Relationship>> paths1 = Util.constructAllPathsToNodeAsRelationships(
                    middleNode, predecessors1, false );
            List<List<Relationship>> paths2 = Util.constructAllPathsToNodeAsRelationships(
                    middleNode, predecessors2, true );
            // For all combinations...
            for ( List<Relationship> part1 : paths1 )
            {
                for ( List<Relationship> part2 : paths2 )
                {
                    // Combine them
                    LinkedList<Relationship> path = new LinkedList<Relationship>();
                    path.addAll( part1 );
                    path.addAll( part2 );
                    // Add to collection
                    paths.add( path );
                }
            }
        }
        return new LinkedList<List<Relationship>>( paths );
    }

    /**
     * @return One of the shortest paths found or null.
     */
    public List<PropertyContainer> getPath()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculate();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return null;
        }
        Node middleNode = foundPathsMiddleNodes.iterator().next();
        LinkedList<PropertyContainer> path = new LinkedList<PropertyContainer>();
        path.addAll( Util.constructSinglePathToNode( middleNode, predecessors1,
                true, false ) );
        path.addAll( Util.constructSinglePathToNode( middleNode, predecessors2,
                false, true ) );
        return path;
    }

    /**
     * @return One of the shortest paths found or null.
     */
    public List<Node> getPathAsNodes()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculate();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return null;
        }
        Node middleNode = foundPathsMiddleNodes.iterator().next();
        LinkedList<Node> pathNodes = new LinkedList<Node>();
        pathNodes.addAll( Util.constructSinglePathToNodeAsNodes( middleNode,
                predecessors1, true, false ) );
        pathNodes.addAll( Util.constructSinglePathToNodeAsNodes( middleNode,
                predecessors2, false, true ) );
        return pathNodes;
    }

    /**
     * @return One of the shortest paths found or null.
     */
    public List<Relationship> getPathAsRelationships()
    {
        if ( startNode == null || endNode == null )
        {
            throw new RuntimeException( "Start or end node undefined." );
        }
        calculate();
        if ( foundPathsMiddleNodes == null || foundPathsMiddleNodes.size() == 0 )
        {
            return null;
        }
        Node middleNode = foundPathsMiddleNodes.iterator().next();
        List<Relationship> path = new LinkedList<Relationship>();
        path.addAll( Util.constructSinglePathToNodeAsRelationships( middleNode,
                predecessors1, false ) );
        path.addAll( Util.constructSinglePathToNodeAsRelationships( middleNode,
                predecessors2, true ) );
        return path;
    }

    /**
     * This sets the maximum depth in the form of a maximum number of
     * relationships to follow.
     *
     * @param maxRelationShipsToTraverse
     */
    public void limitMaxRelationShipsToTraverse( long maxRelationShipsToTraverse )
    {
        this.maxRelationShipsToTraverse = maxRelationShipsToTraverse;
    }

    /**
     * This sets the maximum depth in the form of a maximum number of nodes to
     * scan.
     *
     * @param maxNodesToTraverse
     */
    public void limitMaxNodesToTraverse( long maxNodesToTraverse )
    {
        this.maxNodesToTraverse = maxNodesToTraverse;
    }

    /**
     * Set the end node. Will reset the calculation.
     *
     * @param endNode the endNode to set
     */
    public void setEndNode( Node endNode )
    {
        reset();
        this.endNode = endNode;
    }

    /**
     * Set the start node. Will reset the calculation.
     *
     * @param startNode the startNode to set
     */
    public void setStartNode( Node startNode )
    {
        this.startNode = startNode;
        reset();
    }

    /**
     * @return the relationDirection
     */
    public Direction getDirection()
    {
        return relationDirection;
    }

    /**
     * @return the costRelationType
     */
    public RelationshipType[] getRelationshipTypes()
    {
        return costRelationTypes;
    }

    /**
     * Set the evaluator for pruning the paths when the maximum cost is
     * exceeded.
     *
     * @param maxCost
     */
    public void limitMaxCostToTraverse( CostType maxCost )
    {
        this.maxCost = maxCost;
    }
}
