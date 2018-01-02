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
package org.neo4j.graphalgo.impl.centrality;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.Node;

/**
 * This serves as a base class for all centrality algorithms based on shortest
 * paths. All these algorithms make use of the SingleSourceShortestPath object
 * to calculate the shortest paths which can be adapted for the kind of graph to
 * run the calculation on and set up with proper calculation limits. The main
 * purpose of this class, except containing everything common to these
 * algorithms, is to open up for parallel computation of several of these
 * algorithms at the same time. This could be made possible by reusing the
 * SingleSourceShortestPath computation made for every node.
 * @author Patrik Larsson
 * @param <CentralityType>
 *            The result datatype. Values of this is stored for every node and
 *            accumulated during calculation.
 * @param <ShortestPathCostType>
 *            The datatype used by the SingleSourceShortestPath to represent
 *            path weights.
 */
public abstract class ShortestPathBasedCentrality<CentralityType,ShortestPathCostType>
{
    protected SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath;
    protected CostAccumulator<CentralityType> centralityAccumulator;
    protected CentralityType zeroValue;
    protected Set<Node> nodeSet;
    protected boolean doneCalculation = false;
    /**
     * This map over centrality values is made available to the algorithms
     * inheriting this class. It is supposed to be filled with the method
     * addCentralityToNode.
     */
    protected Map<Node,CentralityType> centralities = null;

    /**
     * Default constructor.
     * @param singleSourceShortestPath
     *            The underlying shortest path algorithm.
     * @param centralityAccumulator
     *            When centralities are built through sums, this makes it
     *            possible to call addCentralityToNode several times, which then
     *            uses this object to add values together.
     * @param zeroValue
     *            The default value to start with.
     * @param nodeSet
     *            The set of nodes values should be stored for.
     */
    public ShortestPathBasedCentrality(
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath,
        CostAccumulator<CentralityType> centralityAccumulator,
        CentralityType zeroValue, Set<Node> nodeSet )
    {
        super();
        this.singleSourceShortestPath = singleSourceShortestPath;
        this.centralityAccumulator = centralityAccumulator;
        this.zeroValue = zeroValue;
        this.nodeSet = nodeSet;
        reset();
    }

    /**
     * The calculation is normally only done once, this resets it so it can be
     * run again. Also used locally for initialization.
     */
    public void reset()
    {
        doneCalculation = false;
        centralities = new HashMap<Node,CentralityType>();
        for ( Node node : nodeSet )
        {
            centralities.put( node, zeroValue );
        }
    }

    /**
     * This adds a value to a given node in the centralities Map. If the Map
     * does not contain the node, it is added.
     * @param node
     * @param value
     */
    protected void addCentralityToNode( Node node, CentralityType value )
    {
        CentralityType centrality = centralities.get( node );
        if ( centrality == null )
        {
            centrality = zeroValue;
        }
        centralities.put( node, centralityAccumulator.addCosts( centrality,
            value ) );
    }

    /**
     * This sets a value for a given node in the centralities Map. If the Map
     * does not contain the node, it is added.
     * @param node
     * @param value
     */
    protected void setCentralityForNode( Node node, CentralityType value )
    {
        centralities.put( node, value );
    }

    /**
     * This can be used to retrieve the result for every node. Will return null
     * if the node is not contained in the node set initially given.
     * @param node
     * @return
     */
    public CentralityType getCentrality( Node node )
    {
        calculate();
        return centralities.get( node );
    }

    /**
     * Runs the calculation. This should not need to be called explicitly, since
     * all attempts to retrieve any kind of result should automatically call
     * this.
     */
    public void calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return;
        }
        doneCalculation = true;
        // For all nodes...
        for ( Node startNode : nodeSet )
        {
            // Prepare the singleSourceShortestPath
            singleSourceShortestPath.reset();
            singleSourceShortestPath.setStartNode( startNode );
            // Process
            processShortestPaths( startNode, singleSourceShortestPath );
        }
    }
    
    /**
     * This method allows to skip the calculation of the centrality via
     * the calculate method. This is to allow user defined calculation of
     * the centralities via the processShortestPaths method.
     */
    public void skipCalculation(){
    	doneCalculation = true;
    }
    
	/**
     * Checks if the calculation is already done
     * @return	status of the calculation
     */
    public boolean isCalculated(){
    	return doneCalculation;
    }

    /**
     * This is the abstract method all centrality algorithms based on this class
     * need to implement. It is called once for every node in the node set,
     * along with a SingleSourceShortestPath starting in that node.
     * @param node
     * @param singleSourceShortestPath
     */
    public abstract void processShortestPaths( Node node,
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath );
}
