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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.Node;

/**
 * This is a utility class used to group together a number of centrality measure
 * calculations to run them all at the same time. Doing this enables us to reuse
 * the results of the underlying {@link SingleSourceShortestPath} algorithm,
 * instead of re-running it for each centrality measure. We do it by collecting
 * a number of {@link ShortestPathBasedCentrality} and then running the
 * {@link SingleSourceShortestPath} for every node.
 * @complexity The sum of the complexities of the centrality measures to
 *             compute, except that all the n*A terms implode into one single
 *             n*A term.
 * @author Patrik Larsson
 * @param <ShortestPathCostType>
 *            The datatype used by the underlying
 *            {@link SingleSourceShortestPath} algorithm, i.e. the type the edge
 *            weights are represented by.
 */
public class ParallellCentralityCalculation<ShortestPathCostType>
{
    protected SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath;
    protected Set<Node> nodeSet;
    protected List<ShortestPathBasedCentrality<?,ShortestPathCostType>> calculations = new LinkedList<ShortestPathBasedCentrality<?,ShortestPathCostType>>();
    protected boolean doneCalculation = false;

    /**
     * Default constructor.
     * @param singleSourceShortestPath
     *            Underlying singleSourceShortestPath.
     * @param nodeSet
     *            A set containing the nodes for which centrality values should
     *            be computed.
     */
    public ParallellCentralityCalculation(
        SingleSourceShortestPath<ShortestPathCostType> singleSourceShortestPath,
        Set<Node> nodeSet )
    {
        super();
        this.singleSourceShortestPath = singleSourceShortestPath;
        this.nodeSet = nodeSet;
    }

    /**
     * This adds a centrality measure to be included in the calculation.
     * @param shortestPathBasedCentrality
     *            The centrality algorithm.
     */
    public void addCalculation(
        ShortestPathBasedCentrality<?,ShortestPathCostType> shortestPathBasedCentrality )
    {
        if ( doneCalculation )
        {
            throw new RuntimeException(
                "Trying to add a centrality calculation to a parallell computation that has already been done." );
        }
        calculations.add( shortestPathBasedCentrality );
        shortestPathBasedCentrality.skipCalculation();
    }

    /**
     * Method that will perform the calculation. After this we are of course
     * unable to add more measures to this object.
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
            for ( ShortestPathBasedCentrality<?,ShortestPathCostType> calculation : calculations )
            {
                calculation.processShortestPaths( startNode,
                    singleSourceShortestPath );
            }
        }
    }
}
