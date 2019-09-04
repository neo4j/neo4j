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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ImpermanentDbmsExtension
class TestConcurrentIteratorModification
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void shouldNotThrowConcurrentModificationExceptionWhenUpdatingWhileIterating()
    {
        // given
        Label label = Label.label( "Bird" );

        Node node1;
        Node node2;
        Node node3;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.createNode( label );
            node2 = tx.createNode( label );
            tx.commit();
        }

        // when
        Set<Node> result = new HashSet<>();
        try ( Transaction tx = db.beginTx() )
        {
            node3 = tx.createNode( label );
            ResourceIterator<Node> iterator = tx.findNodes( label );
            node3.removeLabel( label );
            tx.createNode( label );
            while ( iterator.hasNext() )
            {
                result.add( iterator.next() );
            }
            tx.commit();
        }

        // then does not throw and retains view from iterator creation time
        assertEquals( asSet( node1, node2, node3 ), result );
    }
}
