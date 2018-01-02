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

import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Computing eigenvector centrality with the "power method". Convergence is
 * dependent of the eigenvalues of the input adjacency matrix (the network). If
 * the two largest eigenvalues are u1 and u2, a small factor u2/u1 will give a
 * faster convergence (i.e. faster computation). NOTE: Currently only works on
 * Doubles.
 * @complexity The {@link CostEvaluator} is called once for every relationship
 *             in each iteration. Assuming this is done in constant time, the
 *             total time complexity is O(i(n + m)) when i iterations are done.
 * @author Patrik Larsson
 * @author Anton Persson
 */
public class EigenvectorCentralityPower extends EigenvectorCentralityBase
{
    /**
     * See {@link EigenvectorCentralityBase#EigenvectorCentralityBase(Direction, CostEvaluator, Set, Set, double)}
     */
    public EigenvectorCentralityPower( Direction relationDirection,
        CostEvaluator<Double> costEvaluator, Set<Node> nodeSet,
        Set<Relationship> relationshipSet, double precision )
    {
        super( relationDirection, costEvaluator, nodeSet, relationshipSet, precision );
    }

    public int runInternalIteration()
    {

        incrementTotalIterations();

        Map<Node, Double> newValues = processRelationships();

        normalize( newValues );

        values = newValues;

        return 1;
    }
}
