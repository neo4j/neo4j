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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class TestConcurrentIteratorModification
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule();

    @Test
    public void shouldNotThrowConcurrentModificationExceptionWhenUpdatingWhileIterating()
    {
        // given
        GraphDatabaseService graph = dbRule.getGraphDatabaseAPI();
        Label label = Label.label( "Bird" );

        Node node1, node2, node3;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode( label );
            node2 = graph.createNode( label );
            tx.success();
        }

        // when
        Set<Node> result = new HashSet<>();
        try ( Transaction tx = graph.beginTx() )
        {
            node3 = graph.createNode( label );
            ResourceIterator<Node> iterator = graph.findNodes( label );
            node3.removeLabel( label );
            graph.createNode( label );
            while ( iterator.hasNext() )
            {
                result.add( iterator.next() );
            }
            tx.success();
        }

        // then does not throw and retains view from iterator creation time
        assertEquals(asSet(node1, node2), result);
    }
}
