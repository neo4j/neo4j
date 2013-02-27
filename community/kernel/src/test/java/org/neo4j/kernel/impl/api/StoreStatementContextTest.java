/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;
import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.HashSet;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

public class StoreStatementContextTest
{
    @Test
    public void should_be_able_to_add_label_to_node() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        long nodeId = db.createNode().getId();
        String labelName = "mylabel";
        long labelId = statement.getOrCreateLabelId( labelName );

        // WHEN
        statement.addLabelToNode( labelId, nodeId );
        tx.success();
        tx.finish();

        // THEN
        assertTrue( "Label " + labelName + " wasn't set on " + nodeId, statement.isLabelSetOnNode( labelId, nodeId ) );
    }
    
    @Test
    public void should_be_able_to_list_labels_for_node() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        long nodeId = db.createNode().getId();
        String labelName1 = "mylabel", labelName2 = "myOtherLabel";
        long labelId1 = statement.getOrCreateLabelId( labelName1 );
        long labelId2 = statement.getOrCreateLabelId( labelName2 );
        statement.addLabelToNode( labelId1, nodeId );
        statement.addLabelToNode( labelId2, nodeId );
        tx.success();
        tx.finish();

        // THEN
        Iterable<Long> readLabels = statement.getLabelsForNode( nodeId );
        assertEquals( new HashSet<Long>( asList( labelId1, labelId2 ) ),
                addToCollection( readLabels, new HashSet<Long>() ) );
    }
    
    @Test
    public void should_be_able_to_get_label_name_for_label() throws Exception
    {
        // GIVEN
        String labelName = "LabelName";
        long labelId = statement.getOrCreateLabelId( labelName );

        // WHEN
        String readLabelName = statement.getLabelName( labelId );

        // THEN
        assertEquals( labelName, readLabelName );
    }

    @Test
    public void should_be_able_to_remove_node_label() throws Exception
    {
        // GIVEN
        Transaction tx = db.beginTx();
        String labelName = "MyLabel";
        long labelId = statement.getOrCreateLabelId( labelName );
        long node = db.createNode().getId();
        statement.addLabelToNode( labelId, node );
        tx.success();
        tx.finish();

        // WHEN
        tx = db.beginTx();
        statement.removeLabelFromNode( labelId, node );
        tx.success();
        tx.finish();

        // THEN
        assertFalse( statement.isLabelSetOnNode( labelId, node ) );
    }

    /*
     * This test doesn't really belong here, but OTOH it does, as it has to do with this specific
     * store solution. It creates its own IGD with cache_type:none to try reproduce to trigger the problem.
     */
    @Test
    public void labels_should_not_leak_out_as_properties() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( cache_type, "none" ).newGraphDatabase();
        Label label = label( "the-label" );
        Node node = createLabeledNode( db, map( "name", "Node" ), label );

        // WHEN
        Iterable<String> propertyKeys = node.getPropertyKeys();
        
        // THEN
        assertEquals( asSet( "name" ), asSet( propertyKeys ) );
    }
    
    @Test
    public void should_return_true_when_adding_new_label() throws Exception
    {
        // GIVEN
        Label label = label( "the-label" );
        Node node = createLabeledNode( db, map() );

        // WHEN
        Transaction tx = db.beginTx();
        boolean added = false;
        try
        {
            long labelId = statement.getOrCreateLabelId( label.name() );
            added = statement.addLabelToNode( labelId, node.getId() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertTrue( "Label should have been added", added );
    }
    
    @Test
    public void should_return_false_when_adding_existing_label() throws Exception
    {
        // GIVEN
        Label label = label( "the-label" );
        Node node = createLabeledNode( db, map(), label );

        // WHEN
        Transaction tx = db.beginTx();
        boolean added = false;
        try
        {
            long labelId = statement.getOrCreateLabelId( label.name() );
            added = statement.addLabelToNode( labelId, node.getId() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertFalse( "Label should not have been added", added );
    }
    
    @Test
    public void should_return_true_when_remove_existing_label() throws Exception
    {
        // GIVEN
        Label label = label( "the-label" );
        Node node = createLabeledNode( db, map(), label );

        // WHEN
        Transaction tx = db.beginTx();
        boolean removed = false;
        try
        {
            long labelId = statement.getOrCreateLabelId( label.name() );
            removed = statement.removeLabelFromNode( labelId, node.getId() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertTrue( "Label should have been removed", removed );
    }
    
    @Test
    public void should_return_false_when_removing_non_existent_label() throws Exception
    {
        // GIVEN
        Label label = label( "the-label" );
        Node node = createLabeledNode( db, map() );

        // WHEN
        Transaction tx = db.beginTx();
        boolean removed = false;
        try
        {
            long labelId = statement.getOrCreateLabelId( label.name() );
            removed = statement.addLabelToNode( labelId, node.getId() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertTrue( "Label should not have been removed", removed );
    }
    
    @Test
    public void should_return_all_nodes_with_label() throws Exception
    {
        // GIVEN
        Label label1 = label( "first-label" );
        Label label2 = label( "second-label" );
        Node node1 = createLabeledNode( db, map( "name", "First", "age", 1L ), label1 );
        Node node2 = createLabeledNode( db, map( "type", "Node", "count", 10 ), label1, label2 );

        // WHEN
        Iterable<Long> nodesForLabel1 = statement.getNodesWithLabel( statement.getLabelId( label1.name() ) );
        Iterable<Long> nodesForLabel2 = statement.getNodesWithLabel( statement.getLabelId( label2.name() ) );

        // THEN
        assertEquals( asSet( node1.getId(), node2.getId() ), asSet( nodesForLabel1 ) );
        assertEquals( asSet( node2.getId() ), asSet( nodesForLabel2 ) );
    }
    
    @Test
    public void should_create_property_key_if_not_exists() throws Exception
    {
        // GIVEN
        String propertyKey = "name";

        // WHEN
        long id = statement.getOrCreatePropertyKeyId( propertyKey );

        // THEN
        assertTrue( "Should have created a non-negative id", id >= 0 );
    }
    
    @Test
    public void should_get_previously_created_property_key() throws Exception
    {
        // GIVEN
        String propertyKey = "name";
        long id = statement.getOrCreatePropertyKeyId( propertyKey );

        // WHEN
        long secondId = statement.getPropertyKeyId( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }
    
    @Test
    public void should_be_able_to_get_or_create_previously_created_property_key() throws Exception
    {
        // GIVEN
        String propertyKey = "name";
        long id = statement.getOrCreatePropertyKeyId( propertyKey );

        // WHEN
        long secondId = statement.getOrCreatePropertyKeyId( propertyKey );

        // THEN
        assertEquals( id, secondId );
    }
    
    @Test
    public void should_fail_if_get_non_existent_property_key() throws Exception
    {
        // WHEN
        try
        {
            statement.getPropertyKeyId( "non-existent-property-key" );
            fail( "Should have failed with property key not found exception" );
        }
        catch ( PropertyKeyNotFoundException e )
        {
            // Good
        }
    }
    
    private GraphDatabaseAPI db;
    private StatementContext statement;

    @Before
    public void before()
    {
        db = new ImpermanentGraphDatabase();
        statement = new StoreStatementContext(
                db.getDependencyResolver().resolveDependency( PropertyIndexManager.class ),
                db.getDependencyResolver().resolveDependency( PersistenceManager.class ),
                // Ooh, jucky
                db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                        .getNeoStoreDataSource().getNeoStore(),
                db.getDependencyResolver().resolveDependency( IndexingService.class ));
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    private static Node createLabeledNode( GraphDatabaseService db, Map<String, Object> properties, Label... labels )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
                node.setProperty( property.getKey(), property.getValue() );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
}
