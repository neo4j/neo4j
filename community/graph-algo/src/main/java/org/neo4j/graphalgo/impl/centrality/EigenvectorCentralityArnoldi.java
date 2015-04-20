/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.util.MatrixUtil;
import org.neo4j.graphalgo.impl.util.MatrixUtil.DoubleMatrix;
import org.neo4j.graphalgo.impl.util.MatrixUtil.DoubleVector;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Computing eigenvector centrality with the "Arnoldi iteration". Convergence is
 * dependent of the eigenvalues of the input adjacency matrix (the network). If
 * the two largest eigenvalues are u1 and u2, a small factor u2/u1 will give a
 * faster convergence (i.e. faster computation). NOTE: Currently only works on
 * Doubles.
 * @complexity The {@link CostEvaluator} is called once for every relationship
 *             in each iteration. Assuming this is done in constant time, the
 *             total time complexity is O(j(n + m + i)) when j internal restarts
 *             are required and i iterations are done in the internal
 *             eigenvector solving of the H matrix. Typically j = the number of
 *             iterations / k, where normally k = 3.
 * @author Patrik Larsson
 */
public class EigenvectorCentralityArnoldi implements EigenvectorCentrality
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
     *            Precision factor (ex. 0.01 for 1% error). Note that this is
     *            not the error from the correct values, but the amount of
     *            change tolerated in one iteration.
     */
    public EigenvectorCentralityArnoldi( Direction relationDirection,
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
     * be called externally to manually trigger the calculation.The calculation is
     * done the first time this method is run. Upon successive requests, the old
     * result is returned, unless the calculation is reset via {@link #reset()}
     */
    public void calculate()
    {
        // Don't do it more than once
        if ( doneCalculation )
        {
            return;
        }
        doneCalculation = true;
        values = new HashMap<Node,Double>();
        totalIterations = 0;
        // generate a random start vector
        Random random = new Random( System.currentTimeMillis() );
        for ( Node node : nodeSet )
        {
            values.put( node, random.nextDouble() );
        }
        normalize( values );
        runIterations( maxIterations );
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
            if ( newValues.get( node ) == null )
            {
                return false;
            }
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
        while ( localIterations < maxNrIterations )
        {
            Map<Node,Double> oldValues = values;
            localIterations += runInternalArnoldi( 3 );
            if ( timeToStop( oldValues, values ) )
            {
                break;
            }
        }
        // If the first value is negative (possibly the whole vector), negate
        // the whole vector
        if ( values.get( nodeSet.iterator().next() ) < 0 )
        {
            for ( Node node : nodeSet )
            {
                values.put( node, -values.get( node ) );
            }
        }
        return localIterations;
    }

    /**
     * This runs the Arnoldi decomposition in a specified number of steps.
     * @param iterations
     *            The number of steps to perform, i.e. the dimension of the H
     *            matrix.
     * @return The number of iterations actually performed. This can be less
     *         than the input argument if the starting vector is not linearly
     *         dependent on that many eigenvectors.
     */
    protected int runInternalArnoldi( int iterations )
    {
        // Create a list of the nodes, in order to quickly translate an index
        // into a node.
        ArrayList<Node> nodes = new ArrayList<Node>( nodeSet.size() );
        for ( Node node : nodeSet )
        {
            nodes.add( node );
        }
        DoubleMatrix hMatrix = new DoubleMatrix();
        DoubleMatrix qMatrix = new DoubleMatrix();
        for ( int i = 0; i < nodes.size(); ++i )
        {
            qMatrix.set( 0, i, values.get( nodes.get( i ) ) );
        }
        int localIterations = 1;
        // The main arnoldi iteration loop
        while ( true )
        {
            ++totalIterations;
            Map<Node,Double> newValues = new HashMap<Node,Double>();
            // "matrix multiplication"
            for ( Relationship relationship : relationshipSet )
            {
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.OUTGOING ) )
                {
                    processRelationship( newValues, relationship, false );
                }
                if ( relationDirection.equals( Direction.BOTH )
                    || relationDirection.equals( Direction.INCOMING ) )
                {
                    processRelationship( newValues, relationship, true );
                }
            }
            // Orthogonalize
            for ( int j = 0; j < localIterations; ++j )
            {
                DoubleVector qj = qMatrix.getRow( j );
                // vector product
                double product = 0;
                for ( int i = 0; i < nodes.size(); ++i )
                {
                    Double d1 = newValues.get( nodes.get( i ) );
                    Double d2 = qj.get( i );
                    if ( d1 != null && d2 != null )
                    {
                        product += d1 * d2;
                    }
                }
                hMatrix.set( j, localIterations - 1, product );
                if ( product != 0.0 )
                {
                    // vector subtraction
                    for ( int i = 0; i < nodes.size(); ++i )
                    {
                        Node node = nodes.get( i );
                        Double value = newValues.get( node );
                        if ( value == null )
                        {
                            value = 0.0;
                        }
                        Double qValue = qj.get( i );
                        if ( qValue != null )
                        {
                            newValues.put( node, value - product * qValue );
                        }
                    }
                }
            }
            double normalizeFactor = normalize( newValues );
            values = newValues;
            DoubleVector qVector = new DoubleVector();
            for ( int i = 0; i < nodes.size(); ++i )
            {
                qVector.set( i, newValues.get( nodes.get( i ) ) );
            }
            qMatrix.setRow( localIterations, qVector );
            if ( normalizeFactor == 0.0 || localIterations >= nodeSet.size()
                || localIterations >= iterations )
            {
                break;
            }
            hMatrix.set( localIterations, localIterations - 1, normalizeFactor );
            ++localIterations;
        }
        // employ the power method to find eigenvector to h
        Random random = new Random( System.currentTimeMillis() );
        DoubleVector vector = new DoubleVector();
        for ( int i = 0; i < nodeSet.size(); ++i )
        {
            vector.set( i, random.nextDouble() );
        }
        MatrixUtil.normalize( vector );
        boolean powerDone = false;
        int its = 0;
        double powerPrecision = 0.1;
        while ( !powerDone )
        {
            DoubleVector newVector = MatrixUtil.multiply( hMatrix, vector );
            MatrixUtil.normalize( newVector );
            powerDone = true;
            for ( Integer index : vector.getIndices() )
            {
                if ( newVector.get( index ) == null )
                {
                    continue;
                }
                double factor = Math.abs( newVector.get( index )
                    / vector.get( index ) );
                if ( factor - powerPrecision > 1.0
                    || factor + powerPrecision < 1.0 )
                {
                    powerDone = false;
                    break;
                }
            }
            vector = newVector;
            ++its;
            if ( its > 100 )
            {
                break;
            }
        }
        // multiply q and vector to get a ritz vector
        DoubleVector ritzVector = new DoubleVector();
        for ( int r = 0; r < nodeSet.size(); ++r )
        {
            for ( int c = 0; c < localIterations; ++c )
            {
                ritzVector.incrementValue( r, vector.get( c )
                    * qMatrix.get( c, r ) );
            }
        }
        for ( int i = 0; i < nodeSet.size(); ++i )
        {
            values.put( nodes.get( i ), ritzVector.get( i ) );
        }
        normalize( values );
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
                        * costEvaluator.getCost( relationship,
                                backwards ? Direction.INCOMING
                                        : Direction.OUTGOING );
        }
        newValues.put( endNode, newValue );
    }

    /**
     * Normalizes a vector represented as a Map.
     * @param vector
     * @return the initial length of the vector.
     */
    protected double normalize( Map<Node,Double> vector )
    {
        // Compute vector length
        double sum = 0;
        for ( Node node : vector.keySet() )
        {
            Double d = vector.get( node );
            if ( d == null )
            {
                d = 0.0;
                vector.put( node, 0.0 );
            }
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
        return sum;
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
     * Limit the maximum number of iterations to run. Per default,
     * the maximum iterations are set to Integer.MAX_VALUE, which should
     * be limited to 50-100 normally.
     * @param maxIterations
     *            the maxIterations to set
     */
    public void setMaxIterations( int maxIterations )
    {
        this.maxIterations = maxIterations;
    }
}
