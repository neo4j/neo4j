/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

public class DepthZeroTraversalTest extends AbstractTestBase
{
    @BeforeClass
    public static void createTheGraph()
    {
        createGraph( "0 ROOT 1", "1 KNOWS 2", "2 KNOWS 3", "2 KNOWS 4",
                "4 KNOWS 5", "5 KNOWS 6", "3 KNOWS 1" );
    }

    @Test
    @Ignore
    public void shouldGetStartNodeOnDepthZero()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.atDepth( 0 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "6" );
    }

    @Test
    public void shouldGetCorrectNodesAtDepthOne()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.fromDepth( 1 ) ).evaluator( Evaluators.toDepth( 1 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "5" );
    }
    @Test
    @Ignore
    public void shouldGetCorrectNodesAtDepthZero()
    {
        TraversalDescription description = Traversal.description().evaluator(
                Evaluators.fromDepth( 0 ) ).evaluator( Evaluators.toDepth( 0 ) );
        expectNodes( description.traverse( getNodeWithName( "6" ) ), "6" );
    }
}
