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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @author Patrik Larsson
 * @author Anton Persson
 */
public abstract class EigenvectorCentralityBase implements EigenvectorCentrality
{
    protected final Direction relationDirection;
    protected final CostEvaluator<Double> costEvaluator;
    protected final Set<Relationship> relationshipSet;
    protected final double precision;
    protected Set<Node> nodeSet;
    protected boolean doneCalculation = false;
    protected Map<Node,Double> values;
    protected int totalIterations = 0;
    protected int maxIterations = Integer.MAX_VALUE;

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
    public EigenvectorCentralityBase( Direction relationDirection,
            CostEvaluator<Double> costEvaluator, Set<Node> nodeSet,
            Set<Relationship> relationshipSet, double precision )
    {
        this.relationDirection = relationDirection;
        this.costEvaluator = costEvaluator;
        this.nodeSet = nodeSet;
        this.relationshipSet = relationshipSet;
        this.precision = precision;
    }

    /**
     * This can be used to retrieve the result for every node. Will return null
     * if the node is not contained in the node set initially given.
     * Will return {@link #DETACHED_VERTEX_CENTRALITY} for detached vertices. The calculation is
     * done the first time this method is run. Upon successive requests, the old
     * result is returned, unless the calculation is reset via {@link #reset()}
     * @param node to calculate centrality for
     * @return centrality for {@param node}
     */
    @Override
    public Double getCentrality( Node node )
    {
        if ( !nodeSet.contains( node ) )
        {
            return null;
        }
        if ( !node.hasRelationship() )
        {
            return DETACHED_VERTEX_CENTRALITY;
        }
        calculate();
        return values.get( node );
    }

    /**
     * This resets the calculation if we for some reason would like to redo it.
     */
    @Override
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

        runIterations( maxIterations );
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
    protected int runIterations( int maxNrIterations )
    {
        while ( true ) {
            values = new HashMap<Node,Double>();
            totalIterations = 0;

            // generate a random start vector
            Random random = new Random( System.currentTimeMillis() );
            for ( Node node : nodeSet )
            {
                values.put( node, random.nextDouble() );
            }
            normalize( values );

            if ( maxNrIterations <= 0 )
            {
                return 0;
            }
            int localIterations = 0;

            // Do iterations
            while ( localIterations < maxNrIterations )
            {
                Map<Node,Double> oldValues = values;
                localIterations += runInternalIteration();
                if ( timeToStop( oldValues, values ) )
                {
                    break;
                }
            }

            // Check result
            if ( makeSureValueCorrespondsToMostSignificantEigenvector() )
            {
                return localIterations;
            }
            else
            {
                // Value has not converged to an eigenvector that corresponds to the highest eigenvalue.
                // Restart calculation.
            }
        }
    }

    private boolean makeSureValueCorrespondsToMostSignificantEigenvector()
    {
        int sign = 0;
        int otherSign;
        Iterator<Node> iter = nodeSet.iterator();
        Node next;
        Double value;
        while ( iter.hasNext() )
        {
            next = iter.next();
            value = values.get( next );
            if ( value == null )
            {
                values.put( next, 0d );
                value = 0d;
            }
            otherSign = value < - precision ? -1 : value > precision ? 1 : 0;
            if ( otherSign != 0 )
            {
                if ( sign == 0 )
                {
                    sign = otherSign;
                }
                else
                {
                    if ( sign != otherSign )
                    {
                        return false;
                    }
                }
            }
        }

        // If the first none zero value is negative (possibly the whole vector), negate
        // the whole vector
        if ( sign < 0 )
        {
            for ( Node node : nodeSet )
            {
                values.put( node, -values.get( node ) );
            }
        }
        return true;
    }

    /**
     * Should run iteration and return the number of iterations made.
     * @return Number of iterations.
     */
    protected abstract int runInternalIteration();

    /**
     * Loops over relationships in {@link #relationshipSet} and calls {@link #processRelationship} for each one.
     *
     * {@link #relationshipSet} can be viewed as an adjacency matrix, A, and {@link #values} can be viewed as an
     * arbitrary vector, x. In that case this process simulates a matrix-vector multiplication Ax.
     *
     * Result is returned as a {@link Map<Node,Double>}.
     */
    public Map<Node,Double> processRelationships()
    {
        Map<Node,Double> newValues = new HashMap<>();
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
        return newValues;
    }

    /**
     * Internal method used in the "matrix multiplication" in each iteration.
     */
    protected void processRelationship( Map<Node,Double> newValues,
            Relationship relationship, boolean backwards )
    {
        Node startNode;
        if ( backwards )
        {
            startNode = relationship.getEndNode();
        }
        else
        {
            startNode = relationship.getStartNode();
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
     * Increment the total number of iterations. Used by subclasses.
     */
    protected void incrementTotalIterations()
    {
        totalIterations++;
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
