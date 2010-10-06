/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.experimental;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.graphalgo.MatrixUtil;
import org.neo4j.graphalgo.MatrixUtil.DoubleMatrix;
import org.neo4j.graphalgo.MatrixUtil.DoubleVector;
import org.neo4j.graphalgo.centrality.EigenvectorCentrality;
import org.neo4j.graphalgo.shortestPath.CostEvaluator;

/**
 * Computing eigenvector centrality with the Rayleigh Quotient Iteration method.
 * NOTE: Currently only works on Doubles.
 * @author Patrik Larsson
 */
public class EigenvectorCentralityRayleigh implements EigenvectorCentrality
{
    protected Direction relationDirection;
    protected CostEvaluator<Double> costEvaluator;
    protected Set<Node> nodeSet;
    protected Set<Relationship> relationshipSet;
    protected double precision = 0.001;
    protected boolean doneCalculation = false;
    protected Map<Node,Double> values;
    protected int totalIterations = 0;
    private int maxIterations = Integer.MAX_VALUE;

    /**
     * @param costRelationType
     *            The relationship type to traverse.
     * @param relationDirection
     *            The direction in which the paths should follow the
     *            relationships.
     * @param costEvaluator
     * @see CostEvaluator
     * @param nodeSet
     *            The set of nodes the calculation should be run on.
     * @param relationshipSet
     *            The set of relationships that should be processed.
     * @param precision
     *            Precision factor (ex. 0.01 for 1% error)
     */
    public EigenvectorCentralityRayleigh( Direction relationDirection,
        CostEvaluator<Double> costEvaluator, Set<Node> nodeSet,
        Set<Relationship> relationshipSet, double precision )
    {
        super();
        this.relationDirection = relationDirection;
        this.costEvaluator = costEvaluator;
        this.nodeSet = nodeSet;
        this.relationshipSet = relationshipSet;
        this.precision = precision;
    }

    /**
     * This can be used to retrieve the result for every node. Will return null
     * if the node is not contained in the node set initially given, or doesn't
     * receive a result because no relationship points to it.
     * @param node
     * @return
     */
    public Double getCentrality( Node node )
    {
        calculate();
        return values.get( node );
    }

    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    public void reset()
    {
        doneCalculation = false;
    }

    /**
     * Internal calculate method that will do the calculation. This can however
     * be called externally to manually trigger the calculation.
     */
    public void calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return;
        }
        doneCalculation = true;
        totalIterations = 0;
        while ( totalIterations < maxIterations )
        {
            values = new HashMap<Node,Double>();
            // generate a random start vector
            Random random = new Random( System.currentTimeMillis() );
            for ( Node node : nodeSet )
            {
                values.put( node, random.nextDouble() );
            }
            normalize( values );
            runIterations( maxIterations - totalIterations );
            // Check if the result is good
            if ( values.keySet().size() > 0 )
            {
                Double value = values.get( values.keySet().iterator().next() );
                if ( Double.isInfinite( value ) || Double.isNaN( value ) )
                {
                    // Numerical instability or bad start values led to bad
                    // result, retry
                    continue;
                }
            }
            break;
        }
    }

    /**
     * This runs a number of iterations in the computation and stops when enough
     * precision has been reached. A maximum number of iterations to perform is
     * supplied. NOTE: For maxNrIterations > 0 at least one iteration will be
     * run, regardless if good precision has already been reached or not. This
     * method also ignores the global limit defined by maxIterations.
     * @param maxNrIterations
     *            The maximum number of iterations to run.
     * @return the number of iterations performed. if this is lower than the
     *         given maxNrIterations the desired precision has been reached.
     */
    public int runIterations( int maxNrIterations )
    {
        if ( maxNrIterations <= 0 )
        {
            return 0;
        }
        int localIterations = 0;
        while ( true )
        {
            ++localIterations;
            ++totalIterations;
            // compute the eigenvalue estimation lambda
            Map<Node,Double> tempVector = new HashMap<Node,Double>();
            double lambda = 0.0;
            // "matrix multiplication"
            for ( Relationship relationship : relationshipSet )
            {
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.OUTGOING ) )
                {
                    processRelationship( tempVector, relationship, true );
                }
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.INCOMING ) )
                {
                    processRelationship( tempVector, relationship, false );
                }
            }
            // vector multiplication
            for ( Node node : tempVector.keySet() )
            {
                Double other = values.get( node );
                if ( other == null )
                {
                    continue;
                }
                lambda += other * tempVector.get( node );
            }
            // now run the main matrix magic to get the new values
            DoubleMatrix matrix = new DoubleMatrix();
            // Build a mapping of node -> index
            Map<Node,Integer> indices = new HashMap<Node,Integer>();
            int index = 0;
            for ( Node node : nodeSet )
            {
                indices.put( node, index++ );
            }
            // Build the adjacency matrix
            for ( Relationship relationship : relationshipSet )
            {
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.OUTGOING ) )
                {
                    matrix.set( indices.get( relationship.getEndNode() ),
                        indices.get( relationship.getStartNode() ),
                        costEvaluator.getCost( relationship, true ) );
                }
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.INCOMING ) )
                {
                    matrix.set( indices.get( relationship.getStartNode() ),
                        indices.get( relationship.getEndNode() ), costEvaluator
                            .getCost( relationship, false ) );
                }
            }
            for ( Node node : values.keySet() )
            {
                int i = indices.get( node );
                matrix.incrementValue( i, i, -lambda );
            }
            DoubleVector newValuesVector = new DoubleVector();
            // Copy values to newValuesVector
            for ( Node node : values.keySet() )
            {
                newValuesVector.set( indices.get( node ), values.get( node ) );
            }
            // Do the magic
            MatrixUtil.LinearSolve( matrix, newValuesVector );
            Map<Node,Double> newValues = new HashMap<Node,Double>();
            // Copy values to newValues
            for ( Node node : values.keySet() )
            {
                Double value = newValuesVector.get( indices.get( node ) );
                if ( value != null )
                {
                    newValues.put( node, value );
                }
            }
            // normalize
            normalize( newValues );
            if ( timeToStop( values, newValues ) )
            {
                values = newValues;
                break;
            }
            values = newValues;
            if ( localIterations >= maxNrIterations )
            {
                break;
            }
        }
        if ( values.keySet().size() > 0 )
        {
            if ( values.get( values.keySet().iterator().next() ) < 0 )
            {
                for ( Node node : values.keySet() )
                {
                    values.put( node, -values.get( node ) );
                }
            }
        }
        return localIterations;
    }

    /**
     * Internal method used in the "matrix multiplication" in each iteration.
     */
    protected void processRelationship( Map<Node,Double> newValues,
        Relationship relationship, boolean backwards )
    {
        Node startNode = relationship.getStartNode();
        if ( backwards )
        {
            startNode = relationship.getEndNode();
        }
        Node endNode = relationship.getOtherNode( startNode );
        Double newValue = newValues.get( endNode );
        if ( newValue == null )
        {
            newValue = 0.0;
        }
        if ( values.get( startNode ) != null )
        {
            newValue += values.get( startNode )
                * costEvaluator.getCost( relationship, backwards );
        }
        newValues.put( endNode, newValue );
    }

    /**
     * Stop condition for the iteration.
     * @return true if enough precision has been achieved.
     */
    private boolean timeToStop( Map<Node,Double> oldValues,
        Map<Node,Double> newValues )
    {
        for ( Node node : oldValues.keySet() )
        {
            if ( oldValues.get( node ) == 0.0 )
            {
                if ( Math.abs( newValues.get( node ) ) > precision )
                {
                    return false;
                }
                continue;
            }
            double factor = newValues.get( node ) / oldValues.get( node );
            factor = Math.abs( factor );
            if ( factor - precision > 1.0 || factor + precision < 1.0 )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Normalizes a vector represented as a Map.
     * @param vector
     */
    protected void normalize( Map<Node,Double> vector )
    {
        // Compute vector length
        double sum = 0;
        for ( Node node : vector.keySet() )
        {
            double d = vector.get( node );
            sum += d * d;
        }
        sum = Math.sqrt( sum );
        // Divide all components
        if ( sum > 0.0 )
        {
            for ( Node node : vector.keySet() )
            {
                vector.put( node, vector.get( node ) / sum );
            }
        }
    }

    /**
     * @return the number of iterations made.
     */
    public int getTotalIterations()
    {
        return totalIterations;
    }

    /**
     * @return the maxIterations
     */
    public int getMaxIterations()
    {
        return maxIterations;
    }

    /**
     * Limit the maximum number of iterations to run.
     * @param maxIterations
     *            the maxIterations to set
     */
    public void setMaxIterations( int maxIterations )
    {
        this.maxIterations = maxIterations;
    }
}
