/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.store.id;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseDatabaseRule;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class IdReuseTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EnterpriseDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            super.configure( builder );
            builder.setConfig( EnterpriseEditionSettings.idTypesToReuse, IdType.NODE + "," + IdType.RELATIONSHIP );
        }
    };

    @Test
    public void shouldReuseNodeIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try (Transaction tx = db.beginTx())
        {
            db.createNode();

            tx.failure();
        }

        db = dbRule.restartDatabase();

        // When
        Node node;
        try (Transaction tx = db.beginTx())
        {
            node = db.createNode();

            tx.success();
        }

        // Then
        assertThat(node.getId(), equalTo(0L));
    }

    @Test
    public void shouldReuseRelationshipIdsFromRolledBackTransaction() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Node node1, node2;
        try (Transaction tx = db.beginTx())
        {
            node1 = db.createNode();
            node2 = db.createNode();

            tx.success();
        }

        try (Transaction tx = db.beginTx())
        {
            node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "LIKE" ) );

            tx.failure();
        }

        db = dbRule.restartDatabase();

        // When
        Relationship relationship;
        try (Transaction tx = db.beginTx())
        {
            node1 = db.getNodeById(node1.getId());
            node2 = db.getNodeById(node2.getId());
            relationship = node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "LIKE" ) );

            tx.success();
        }

        // Then
        assertThat(relationship.getId(), equalTo(0L));
    }

    @Test
    public void sequentialOperationNodeIdReuse()
    {
        Label marker = DynamicLabel.label( "marker" );

        long node1 = createNode( marker );
        long node2 = createNode( marker );
        long node3 = createNode( marker );

        assertEquals( "Ids should be sequential", node1 + 1, node2 );
        assertEquals( "Ids should be sequential", node2 + 1, node3 );

        NeoStoreDataSource.BufferedIdMaintenanceController idMaintenanceController = getIdMaintenanceController();

        deleteNodesByLabel( marker );

        idMaintenanceController.maintenance();

        assertEquals( "Nodes have reused id", node1, createNode( marker ) );
        assertEquals( "Nodes have reused id", node2, createNode( marker ) );
        assertEquals( "Nodes have reused id", node3, createNode( marker ) );
    }

    @Test
    public void sequentialOperationRelationshipIdReuse()
    {
        Label marker = DynamicLabel.label( "marker" );

        long relationship1 = createRelationship( marker );
        long relationship2 = createRelationship( marker );
        long relationship3 = createRelationship( marker );

        assertEquals( "Ids should be sequential", relationship1 + 1, relationship2 );
        assertEquals( "Ids should be sequential", relationship2 + 1, relationship3 );

        NeoStoreDataSource.BufferedIdMaintenanceController idMaintenanceController = getIdMaintenanceController();

        deleteRelationshipByLabelAndRelationshipType( marker );

        idMaintenanceController.maintenance();

        assertEquals( "Relationships have reused id", relationship1, createRelationship( marker ) );
        assertEquals( "Relationships have reused id", relationship2, createRelationship( marker ) );
        assertEquals( "Relationships have reused id", relationship3, createRelationship( marker ) );
    }

    @Test
    public void nodeIdReusableOnlyAfterTransactionFinish()
    {
        Label testLabel = DynamicLabel.label( "testLabel" );
        long nodeId = createNode( testLabel );

        NeoStoreDataSource.BufferedIdMaintenanceController idMaintenanceController = getIdMaintenanceController();

        try ( Transaction tx = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( testLabel ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }

            idMaintenanceController.maintenance();

            Node newNode = dbRule.createNode( testLabel );

            assertNotEquals( "Nodes should have different ids.", nodeId, newNode.getId() );
            tx.success();
        }

        idMaintenanceController.maintenance();
        assertEquals( "New node should have id of a removed node", nodeId, createNode( testLabel ) );
    }

    @Test
    public void relationshipIdReusableOnlyAfterTransactionFinish()
    {
        Label testLabel = DynamicLabel.label( "testLabel" );
        long relationshipId = createRelationship( testLabel );

        NeoStoreDataSource.BufferedIdMaintenanceController idMaintenanceController = getIdMaintenanceController();

        try ( Transaction transaction = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( testLabel ) )
        {
            List<Node> nodeList = IteratorUtil.asList( nodes );
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

            assertNotEquals( "Relatioships should have different ids.", relationshipId, relationshipTo.getId() );
            transaction.success();
        }

        idMaintenanceController.maintenance();
        assertEquals( "New relationship should have id of a removed relationship", relationshipId,
                createRelationship( testLabel ) );
    }

    private void deleteNodesByLabel( Label marker )
    {
        try ( Transaction tx = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( marker ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
            tx.success();
        }
    }

    private void deleteRelationshipByLabelAndRelationshipType( Label marker )
    {
        try ( Transaction transaction = dbRule.beginTx();
              ResourceIterator<Node> nodes = dbRule.findNodes( marker ) )
        {
            List<Node> nodeList = IteratorUtil.asList( nodes );
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

    private NeoStoreDataSource.BufferedIdMaintenanceController getIdMaintenanceController()
    {
        return dbRule.getDependencyResolver()
                .resolveDependency( NeoStoreDataSource.BufferedIdMaintenanceController.class );
    }

    private long createNode( Label label )
    {
        try ( Transaction tx = dbRule.beginTx() )
        {
            Node node = dbRule.createNode( label );
            tx.success();
            return node.getId();
        }
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
