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

import java.util.ArrayList;
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
 * @author Anton Persson
 */
public class EigenvectorCentralityArnoldi extends EigenvectorCentralityBase
{
    /**
     * See {@link EigenvectorCentralityBase#EigenvectorCentralityBase(Direction, CostEvaluator, Set, Set, double)}
     */
    public EigenvectorCentralityArnoldi( Direction relationDirection,
        CostEvaluator<Double> costEvaluator, Set<Node> nodeSet,
        Set<Relationship> relationshipSet, double precision )
    {
        super( relationDirection, costEvaluator, nodeSet, relationshipSet, precision );
    }

    /**
     * This runs the Arnoldi decomposition in a specified number of steps.
     */
    @Override
    protected int runInternalIteration()
    {
        int iterations = 3;
        // Create a list of the nodes, in order to quickly translate an index
        // into a node.
        ArrayList<Node> nodes = new ArrayList<>( nodeSet.size() );
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
            incrementTotalIterations();

            Map<Node, Double> newValues = processRelationships();

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
                Node key = nodes.get( i );
                Double value = newValues.get( key );
                if ( value != null )
                {
                    qVector.set( i, value );
                }
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
}
