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
package org.neo4j.graphdb;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.createIndex;
import static org.neo4j.graphdb.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.graphdb.Neo4jMatchers.waitForIndex;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.MapUtil.map;

public class IndexingAcceptanceTest
{
    @Test
    public void shouldHandleAddingDataToAsWellAsDeletingIndexInTheSameTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        IndexDefinition index = null;
        String key = "key";
        {
            Transaction tx = beansAPI.beginTx();
            try
            {
                Node node = beansAPI.createNode( MY_LABEL );
                node.setProperty( key, "value" );
                index = beansAPI.schema().indexFor( MY_LABEL ).on( key ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            waitForIndex( beansAPI, index );
        }

        // WHEN
        Transaction tx = beansAPI.beginTx();
        try
        {
            Node node = beansAPI.createNode( MY_LABEL );
            node.setProperty( key, "other value" );
            index.drop();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        tx = beansAPI.beginTx();
        try
        {
            assertEquals( emptySetOf( IndexDefinition.class ), asSet( beansAPI.schema().getIndexes( MY_LABEL ) ) );
            beansAPI.schema().getIndexState( index );
            fail( "Should not succeed" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), containsString( MY_LABEL.name() ) );
        }
        finally
        {
            tx.finish();
        }
    }
    
    /* This test is a bit interesting. It tests a case where we've got a property that sits in one
     * property block and the value is of a long type. So given that plus that there's an index for that
     * label/property, do an update that changes the long value into a value that requires two property blocks.
     * This is interesting because the transaction logic compares before/after views per property record and
     * not per node as a whole.
     * 
     * In this case this change will be converted into one "add" and one "remove" property updates instead of
     * a single "change" property update. At the very basic level it's nice to test for this corner-case so
     * that the externally observed behavior is correct, even if this test doesn't assert anything about
     * the underlying add/remove vs. change internal details.
     */
    @Test
    public void shouldInterpretPropertyAsChangedEvenIfPropertyMovesFromOneRecordToAnother() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();
        long smallValue = 10L, bigValue = 1L << 62;
        Node myNode = null;
        {
            Transaction tx = beansAPI.beginTx();
            IndexDefinition indexDefinition;
            try
            {
                myNode = beansAPI.createNode( MY_LABEL );
                myNode.setProperty( "pad0", true );
                myNode.setProperty( "pad1", true );
                myNode.setProperty( "pad2", true );
                // Use a small long here which will only occupy one property block
                myNode.setProperty( "key", smallValue );

                indexDefinition = beansAPI.schema().indexFor( MY_LABEL ).on( "key" ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            waitForIndex( beansAPI, indexDefinition );
        }

        // WHEN
        Transaction tx = beansAPI.beginTx();
        try
        {
            // A big long value which will occupy two property blocks
            myNode.setProperty( "key", bigValue );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "key", bigValue, beansAPI ), containsOnly( myNode ) );
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "key", smallValue, beansAPI ), isEmpty() );
    }
    
    @Test
    public void shouldUseDynamicPropertiesToIndexANodeWhenAddedAlongsideExistingPropertiesInASeparateTransaction() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();

        // When
        long id;
        {
            Transaction tx = beansAPI.beginTx();
            IndexDefinition indexDefinition;
            try
            {
                Node myNode = beansAPI.createNode();
                id = myNode.getId();
                myNode.setProperty( "key0", true );
                myNode.setProperty( "key1", true );

                indexDefinition = beansAPI.schema().indexFor( MY_LABEL ).on( "key2" ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            waitForIndex( beansAPI, indexDefinition );
        }
        Node myNode;
        {
            Transaction tx = beansAPI.beginTx();
            myNode = beansAPI.getNodeById( id );
            try
            {
                myNode.addLabel( MY_LABEL );
                myNode.setProperty( "key2", LONG_STRING );
                myNode.setProperty( "key3", LONG_STRING );

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        // Then
        assertThat( myNode, inTx( beansAPI, hasProperty( "key2" ).withValue( LONG_STRING ) ) );
        assertThat( myNode, inTx( beansAPI, hasProperty( "key3" ).withValue( LONG_STRING ) ) );
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "key2", LONG_STRING, beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingForNodeByPropertyShouldWorkWithoutIndex() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), MY_LABEL );

        // When
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "name", "Hawking", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingUsesIndexWhenItExists() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), MY_LABEL );
        createIndex( beansAPI, MY_LABEL, "name" );

        // When
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "name", "Hawking", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When/Then
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "name", "Hawking", beansAPI ), isEmpty() );
    }

    @Test
    public void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, MY_LABEL, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), MY_LABEL );

        // WHEN THEN
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias", beansAPI ), containsOnly( firstNode ) );
        Node secondNode = createNode( beansAPI, map( "name", "Taylor" ), MY_LABEL );
        assertThat( findNodesByLabelAndProperty( MY_LABEL, "name", "Taylor", beansAPI ), containsOnly( secondNode ) );
    }

    @Test
    public void createdNodeShouldShowUpWithinTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, MY_LABEL, "name" );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), MY_LABEL );
        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );

        tx.finish();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(0l) );
    }

    @Test
    public void deletedNodeShouldShowUpWithinTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, MY_LABEL, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), MY_LABEL );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );

        tx.finish();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(0l) );
    }

    @Test
    public void createdNodeShouldShowUpInIndexQuery() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, MY_LABEL, "name" );
        createNode( beansAPI, map( "name", "Mattias" ), MY_LABEL );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );
        createNode( beansAPI, map( "name", "Mattias" ), MY_LABEL );
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( MY_LABEL, "name", "Mattias" ) );

        tx.finish();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(2l) );
    }

    public static final String LONG_STRING = "a long string that has to be stored in dynamic records";
    
    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private Label MY_LABEL = DynamicLabel.label( "MY_LABEL" );

    private Node createNode( GraphDatabaseService beansAPI, Map<String, Object> properties, Label... labels )
    {
        Transaction tx = beansAPI.beginTx();
        try
        {
            Node node = beansAPI.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
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
