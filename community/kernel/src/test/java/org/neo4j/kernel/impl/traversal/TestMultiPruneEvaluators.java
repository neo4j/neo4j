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
package org.neo4j.kernel.impl.traversal;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.traversal.Evaluators.toDepth;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.kernel.Traversal.traversal;

public class TestMultiPruneEvaluators extends TraversalTestBase
{
    @Before
    public void setupGraph()
    {
        createGraph( "a to b", "a to c", "a to d", "a to e",
                "b to f", "b to g", "b to h",
                "c to i",
                "d to j", "d to k", "d to l",
                "e to m", "e to n",
                "k to o", "k to p", "k to q", "k to r" );
    }

    @Test
    public void testMaxDepthAndCustomPruneEvaluatorCombined()
    {
        Evaluator lessThanThreeRels = new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return count( path.endNode().getRelationships( Direction.OUTGOING ).iterator() ) < 3 ?
                        Evaluation.INCLUDE_AND_PRUNE : Evaluation.INCLUDE_AND_CONTINUE;
            }
        };

        TraversalDescription description = traversal().evaluator( Evaluators.all() )
                .evaluator( toDepth( 1 ) ).evaluator( lessThanThreeRels );
        Set<String> expectedNodes = new HashSet<String>(
                asList( "a", "b", "c", "d", "e" ) );
        try ( Transaction tx = beginTx() )
        {
            for ( Path position : description.traverse( node( "a" ) ) )
            {
                String name = (String) position.endNode().getProperty( "name" );
                assertTrue( name + " shouldn't have been returned", expectedNodes.remove( name ) );
            }
            tx.success();
        }
        assertTrue( expectedNodes.isEmpty() );
    }
}
