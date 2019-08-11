/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;

import static org.neo4j.graphdb.traversal.Evaluators.atDepth;

class DepthOneTraversalTest extends TraversalTestBase
{
    private Transaction tx;

    @BeforeEach
    void createTheGraph()
    {
        createGraph( "0 ROOT 1", "1 KNOWS 2", "2 KNOWS 3", "2 KNOWS 4",
                "4 KNOWS 5", "5 KNOWS 6", "3 KNOWS 1" );
        tx = beginTx();
    }

    @AfterEach
    void tearDown()
    {
        tx.close();
    }

    private void shouldGetBothNodesOnDepthOne( TraversalDescription description )
    {
        description = description.evaluator( atDepth( 1 ) );
        expectNodes( description.traverse( getNodeWithName( "3" ) ), "1", "2" );
    }

    @Test
    void shouldGetBothNodesOnDepthOneForDepthFirst()
    {
        shouldGetBothNodesOnDepthOne( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void shouldGetBothNodesOnDepthOneForBreadthFirst()
    {
        shouldGetBothNodesOnDepthOne( getGraphDb().traversalDescription().breadthFirst() );
    }
}
