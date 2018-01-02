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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

public class SpecificDepthTraversalTest extends TraversalTestBase
{
    private Transaction tx;

    @Before
    public void createTheGraph()
    {
        createGraph( "0 ROOT 1", "1 KNOWS 2", "2 KNOWS 3", "2 KNOWS 4",
                "4 KNOWS 5", "5 KNOWS 6", "3 KNOWS 1" );
        tx = beginTx();
    }

    @After
    public void tearDown()
    {
        tx.close();
    }

    @Test
    public void shouldGetStartNodeOnDepthZero()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.atDepth( 0 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "6" );
    }

    @Test
    public void shouldGetCorrectNodesFromToDepthOne()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.fromDepth( 1 ) ).evaluator( Evaluators.toDepth( 1 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "5" );
    }

    @Test
    public void shouldGetCorrectNodeAtDepthOne()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.atDepth( 1 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "5" );
    }

    @Test
    public void shouldGetCorrectNodesAtDepthZero()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.fromDepth( 0 ) ).evaluator( Evaluators.toDepth( 0 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "6" );
    }

    @Test
    public void shouldGetStartNodeWhenFromToIsZeroBreadthFirst()
    {
        TraversalDescription description = Traversal.description().breadthFirst()
                .evaluator(Evaluators.fromDepth(0)).evaluator(Evaluators.toDepth(0));

        expectNodes( description.traverse( getNodeWithName( "0" ) ), "0" );
    }

    @Test
    public void shouldGetStartNodeWhenAtIsZeroBreadthFirst()
    {
        TraversalDescription description = Traversal.description().breadthFirst()
                .evaluator(Evaluators.atDepth(0));

        expectNodes( description.traverse( getNodeWithName( "2" ) ), "2" );
    }

    @Test
    public void shouldGetSecondNodeWhenFromToIsTwoBreadthFirst()
    {
        TraversalDescription description = Traversal.description().breadthFirst()
                .evaluator(Evaluators.fromDepth(2)).evaluator(Evaluators.toDepth(2));

        expectNodes( description.traverse( getNodeWithName( "5" ) ), "2" );
    }

    @Test
    public void shouldGetSecondNodeWhenAtIsTwoBreadthFirst()
    {
        TraversalDescription description = Traversal.description().breadthFirst()
                .evaluator( Evaluators.atDepth( 2 ) );

        expectNodes( description.traverse( getNodeWithName( "6" ) ), "4" );
    }
}
