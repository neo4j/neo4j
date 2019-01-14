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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;

public class DenseNodeRelChainPositionTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    /*
     * Tests for a particular bug with dense nodes. It used to be that if a dense node had relationships
     * for only one direction, if a request for relationships of the other direction was made, no relationships
     * would be returned, ever.
     */
    @Test
    public void givenDenseNodeWhenAskForWrongDirectionThenIncorrectNrOfRelsReturned()
    {
        // Given
        final int denseNodeThreshold =
                Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() )
                + 1 // We must be over the dense node threshold for the bug to manifest
                ;

        Node node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            Node node2 = db.createNode();

            for ( int i = 0; i < denseNodeThreshold; i++ )
            {
                node1.createRelationshipTo( node2, RelationshipType.withName( "FOO" ) );
            }
            tx.success();
        }

        // When/Then
        try ( Transaction ignored = db.beginTx() )
        {
            Node node1b = db.getNodeById( node1.getId() );

            Iterable<Relationship> rels = node1b.getRelationships( Direction.INCOMING );
            assertEquals( 0, Iterables.count( rels ) );

            Iterable<Relationship> rels2 = node1b.getRelationships( Direction.OUTGOING );
            assertEquals( denseNodeThreshold, Iterables.count( rels2 ) );
        }
    }
}
