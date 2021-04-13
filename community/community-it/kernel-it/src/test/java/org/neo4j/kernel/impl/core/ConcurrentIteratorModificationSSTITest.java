/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@ImpermanentDbmsExtension( configurationCallback = "configuration" )
class ConcurrentIteratorModificationSSTITest extends TestConcurrentIteratorModification
{
    @Inject
    private GraphDatabaseService db;

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

    @Test
    void shouldNotThrowConcurrentModificationExceptionWhenUpdatingWhileIteratingRelationships()
    {
        // given
        RelationshipType type = RelationshipType.withName( "type" );

        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode();
            Node node2 = tx.createNode();
            rel1 = node1.createRelationshipTo( node2, type );
            rel2 = node2.createRelationshipTo( node1, type );
            tx.commit();
        }

        // when
        Set<Relationship> result = new HashSet<>();
        try ( Transaction tx = db.beginTx() )
        {
            rel3 = tx.createNode().createRelationshipTo( tx.createNode(), type );
            ResourceIterator<Relationship> iterator = tx.findRelationships( type );
            rel3.delete();
            tx.createNode().createRelationshipTo( tx.createNode(), type );
            while ( iterator.hasNext() )
            {
                result.add( iterator.next() );
            }
            tx.commit();
        }

        // then does not throw and retains view from iterator creation time
        assertThat( result ).containsExactlyInAnyOrder( rel1, rel2, rel3 );
    }
}
