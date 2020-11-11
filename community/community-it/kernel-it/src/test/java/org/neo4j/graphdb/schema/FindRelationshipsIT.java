/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb.schema;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.index.label.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@DbmsExtension( configurationCallback = "configuration" )
public class FindRelationshipsIT
{
    private static final RelationshipType REL_TYPE = RelationshipType.withName( "REL_TYPE" );
    private static final RelationshipType OTHER_REL_TYPE = RelationshipType.withName( "OTHER_REL_TYPE" );

    @Inject
    GraphDatabaseService db;
    @Inject
    DbmsController dbmsController;

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
    }

    @Test
    void findRelationshipsWhenTypeNotExistShouldGiveEmptyIterator()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx();
              ResourceIterator<Relationship> relationships = tx.findRelationships( REL_TYPE ) )
        {
            assertFalse( relationships.hasNext() );
        }
    }

    @Test
    void findRelationshipsShouldGiveAllRelationshipsOfType()
    {
        Relationship rel1;
        Relationship rel2;
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            rel1 = tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            rel2 = tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
            tx.commit();
        }

        List<Relationship> result;
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.findRelationships( REL_TYPE ).stream().collect( Collectors.toList() );
        }
        assertThat( result ).containsExactlyInAnyOrder( rel1, rel2 );
    }

    @Test
    void findRelationshipsShouldIncludeChangesInTx()
    {
        Relationship rel1;
        Relationship rel2;
        Relationship rel3;
        Label label = Label.label( "label" );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );

            // Two relationships we will delete
            Node node = tx.createNode( label );
            node.setProperty( "key", "value" );
            node.createRelationshipTo( tx.createNode(), REL_TYPE );
            node.createRelationshipTo( tx.createNode(), REL_TYPE );

            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            rel1 = tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
            tx.commit();
        }

        List<Relationship> result;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Node node2 = tx.createNode();
            rel2 = node.createRelationshipTo( node2, REL_TYPE );
            tx.createNode().createRelationshipTo( tx.createNode(), OTHER_REL_TYPE );
            rel3 = node2.createRelationshipTo( node, REL_TYPE );

            Iterable<Relationship> nodeRels = tx.findNode( label, "key", "value" ).getRelationships();
            nodeRels.forEach( Relationship::delete );

            result = tx.findRelationships( REL_TYPE ).stream().collect( Collectors.toList() );
        }
        assertThat( result ).containsExactlyInAnyOrder( rel1, rel2, rel3 );
    }

    @Test
    void findRelationshipsThrowsIfNoRTSS()
    {
        // Create a relationship so the relationship type exist
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode().createRelationshipTo( tx.createNode(), REL_TYPE );
            tx.commit();
        }

        dbmsController.restartDbms( builder ->
        {
            builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, false );
            return builder;
        });

        try ( Transaction tx = db.beginTx() )
        {
            IllegalStateException illegalStateException = assertThrows( IllegalStateException.class, () -> tx.findRelationships( REL_TYPE ) );
            assertThat( illegalStateException.getMessage() ).isEqualTo( "Cannot search relationship type scan store when feature is not enabled." );
        }
    }
}
