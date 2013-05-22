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
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.asUniqueSet;
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
        Labels label = Labels.MY_LABEL;
        String key = "key";
        {
            Transaction tx = beansAPI.beginTx();
            try
            {
                Node node = beansAPI.createNode( label );
                node.setProperty( key, "value" );
                index = beansAPI.schema().indexFor( label ).on( key ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            beansAPI.schema().awaitIndexOnline( index, 10, SECONDS );
        }

        // WHEN
        Transaction tx = beansAPI.beginTx();
        try
        {
            Node node = beansAPI.createNode( label );
            node.setProperty( key, "other value" );
            index.drop();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertEquals( emptySetOf( IndexDefinition.class ), asSet( beansAPI.schema().getIndexes( label ) ) );
        try
        {
            beansAPI.schema().getIndexState( index );
            fail( "Should not succeed" );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), CoreMatchers.containsString( label.name() ) );
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
                myNode = beansAPI.createNode( Labels.MY_LABEL );
                myNode.setProperty( "pad0", true );
                myNode.setProperty( "pad1", true );
                myNode.setProperty( "pad2", true );
                // Use a small long here which will only occupy one property block
                myNode.setProperty( "key", smallValue );

                indexDefinition = beansAPI.schema().indexFor( Labels.MY_LABEL ).on( "key" ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            beansAPI.schema().awaitIndexOnline( indexDefinition, 10, SECONDS );
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
        assertEquals( asSet( myNode ),
                      asUniqueSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "key", bigValue ) ) );
        assertEquals( emptySetOf( Node.class ),
                      asUniqueSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "key", smallValue ) ) );
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

                indexDefinition = beansAPI.schema().indexFor( Labels.MY_LABEL ).on( "key2" ).create();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            beansAPI.schema().awaitIndexOnline( indexDefinition, 10, SECONDS );
        }
        {
            Transaction tx = beansAPI.beginTx();
            try
            {
                Node myNode = beansAPI.getNodeById( id );
                myNode.addLabel( Labels.MY_LABEL );
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
        assertEquals( LONG_STRING, beansAPI.getNodeById( id ).getProperty( "key2" ) );
        assertEquals( LONG_STRING, beansAPI.getNodeById( id ).getProperty( "key3" ) );

        Node foundNode = single( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "key2", LONG_STRING ) );
        assertEquals( id, foundNode.getId() );
    }

    @Test
    public void searchingForNodeByPropertyShouldWorkWithoutIndex() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), Labels.MY_LABEL );

        // When
        Node result = single( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );

        // Then
        assertEquals( result, myNode );
    }

    @Test
    public void searchingUsesIndexWhenItExists() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), Labels.MY_LABEL );
        createIndex( beansAPI, Labels.MY_LABEL, "name" );

        // When
        Node result = single( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );

        // Then
        assertEquals( result, myNode );
    }

    @Test
    public void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When/Then
        Iterable<Node> result = beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" );
        assertEquals( emptySetOf( Node.class ), asSet( result ) );
    }
    
    @Test
    public void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, Labels.MY_LABEL, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );

        // WHEN
        Set<Node> firstResult = asSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );
        Node secondNode = createNode( beansAPI, map( "name", "Taylor" ), Labels.MY_LABEL );
        Set<Node> secondResult = asSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Taylor" ) );

        // THEN
        assertEquals( asSet( firstNode ), firstResult );
        assertEquals( asSet( secondNode ), secondResult );
    }

    @Test
    public void createdNodeShouldShowUpWithinTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, Labels.MY_LABEL, "name" );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );
        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );

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
        createIndex( beansAPI, Labels.MY_LABEL, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );

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
        createIndex( beansAPI, Labels.MY_LABEL, "name" );
        createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );
        createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );
        long sizeAfterDelete = count( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );

        tx.finish();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(2l) );
    }

    public static final String LONG_STRING = "a long string that has to be stored in dynamic records";
    
    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    
    private enum Labels implements Label
    {
        MY_LABEL, MY_OTHER_LABEL
    }

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

    private IndexDefinition createIndex( GraphDatabaseService beansAPI, Label label, String property )
    {
        Transaction tx = beansAPI.beginTx();
        IndexDefinition indexDef;
        try
        {
            indexDef = beansAPI.schema().indexFor( label ).on( property ).create();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        beansAPI.schema().awaitIndexOnline( indexDef, 10, SECONDS );
        return indexDef;
    }
}
