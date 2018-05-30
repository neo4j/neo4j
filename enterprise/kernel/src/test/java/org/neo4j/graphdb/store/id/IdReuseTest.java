/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.graphdb.store.id;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class IdReuseTest
{
    @Rule
    public DatabaseRule dbRule = new EnterpriseDatabaseRule()
            .withSetting( EnterpriseEditionSettings.idTypesToReuse, IdType.NODE + "," + IdType.RELATIONSHIP )
            .withSetting( GraphDatabaseSettings.record_id_batch_size, "1" );

    @Test
    public void shouldReuseNodeIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();

            tx.failure();
        }

        db = dbRule.restartDatabase();

        // When
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();

            tx.success();
        }

        // Then
        assertThat( node.getId(), equalTo( 0L ) );
    }

    @Test
    public void shouldReuseRelationshipIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Node node1;
        Node node2;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode();
            node2 = db.createNode();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            node1.createRelationshipTo( node2, RelationshipType.withName( "LIKE" ) );

            tx.failure();
        }

        db = dbRule.restartDatabase();

        // When
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.getNodeById( node1.getId() );
            node2 = db.getNodeById( node2.getId() );
            relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "LIKE" ) );

            tx.success();
        }

        // Then
        assertThat( relationship.getId(), equalTo( 0L ) );
    }

    @Test
    public void sequentialOperationRelationshipIdReuse()
    {
        Label marker = Label.label( "marker" );

        long relationship1 = createRelationship( marker );
        long relationship2 = createRelationship( marker );
        long relationship3 = createRelationship( marker );

        assertEquals( "Ids should be sequential", relationship1 + 1, relationship2 );
        assertEquals( "Ids should be sequential", relationship2 + 1, relationship3 );

        final IdController idMaintenanceController = getIdMaintenanceController();

        deleteRelationshipByLabelAndRelationshipType( marker );

        idMaintenanceController.maintenance();

        assertEquals( "Relationships have reused id", relationship1, createRelationship( marker ) );
        assertEquals( "Relationships have reused id", relationship2, createRelationship( marker ) );
        assertEquals( "Relationships have reused id", relationship3, createRelationship( marker ) );
    }

    @Test
    public void relationshipIdReusableOnlyAfterTransactionFinish()
    {
        Label testLabel = Label.label( "testLabel" );
        long relationshipId = createRelationship( testLabel );

        final IdController idMaintenanceController = getIdMaintenanceController();

        try ( Transaction transaction = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( testLabel ) )
        {
            List<Node> nodeList = Iterators.asList( nodes );
            for ( Node node : nodeList )
            {
                Iterable<Relationship> relationships = node.getRelationships( TestRelationshipType.MARKER );
                for ( Relationship relationship : relationships )
                {
                    relationship.delete();
                }
            }

            idMaintenanceController.maintenance();

            Node node1 = dbRule.createNode( testLabel );
            Node node2 = dbRule.createNode( testLabel );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );

            assertNotEquals( "Relationships should have different ids.", relationshipId, relationshipTo.getId() );
            transaction.success();
        }
    }

    private void deleteRelationshipByLabelAndRelationshipType( Label marker )
    {
        try ( Transaction transaction = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( marker ) )
        {
            List<Node> nodeList = Iterators.asList( nodes );
            for ( Node node : nodeList )
            {
                Iterable<Relationship> relationships = node.getRelationships( TestRelationshipType.MARKER );
                for ( Relationship relationship : relationships )
                {
                    relationship.delete();
                }
            }
            transaction.success();
        }
    }

    private IdController getIdMaintenanceController()
    {
        return dbRule.getDependencyResolver().resolveDependency( IdController.class );
    }

    private long createRelationship( Label label )
    {
        try ( Transaction transaction = dbRule.beginTx() )
        {
            Node node1 = dbRule.createNode( label );
            Node node2 = dbRule.createNode( label );

            Relationship relationshipTo = node1.createRelationshipTo( node2, TestRelationshipType.MARKER );
            transaction.success();
            return relationshipTo.getId();
        }
    }

    private enum TestRelationshipType implements RelationshipType
    {
        MARKER
    }
}
