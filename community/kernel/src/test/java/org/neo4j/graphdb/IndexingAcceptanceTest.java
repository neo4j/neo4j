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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.graphdb.Neo4jMatchers.waitForIndex;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.map;

public class IndexingAcceptanceTest
{
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
        Node myNode;
        {
            try ( Transaction tx = beansAPI.beginTx() )
            {
                myNode = beansAPI.createNode( LABEL1 );
                myNode.setProperty( "pad0", true );
                myNode.setProperty( "pad1", true );
                myNode.setProperty( "pad2", true );
                // Use a small long here which will only occupy one property block
                myNode.setProperty( "key", smallValue );

                tx.success();
            }
        }
        {
            IndexDefinition indexDefinition;
            try ( Transaction tx = beansAPI.beginTx() )
            {
                indexDefinition = beansAPI.schema().indexFor( LABEL1 ).on( "key" ).create();

                tx.success();
            }
            waitForIndex( beansAPI, indexDefinition );
        }

        // WHEN
        try ( Transaction tx = beansAPI.beginTx() )
        {
            // A big long value which will occupy two property blocks
            myNode.setProperty( "key", bigValue );
            tx.success();
        }

        // THEN
        assertThat( findNodesByLabelAndProperty( LABEL1, "key", bigValue, beansAPI ), containsOnly( myNode ) );
        assertThat( findNodesByLabelAndProperty( LABEL1, "key", smallValue, beansAPI ), isEmpty() );
    }

    @Test
    public void shouldUseDynamicPropertiesToIndexANodeWhenAddedAlongsideExistingPropertiesInASeparateTransaction() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseAPI();

        // When
        long id;
        {
            try ( Transaction tx = beansAPI.beginTx() )
            {
                Node myNode = beansAPI.createNode();
                id = myNode.getId();
                myNode.setProperty( "key0", true );
                myNode.setProperty( "key1", true );

                tx.success();
            }
        }
        {
            IndexDefinition indexDefinition;
            try ( Transaction tx = beansAPI.beginTx() )
            {
                indexDefinition = beansAPI.schema().indexFor( LABEL1 ).on( "key2" ).create();

                tx.success();
            }
            waitForIndex( beansAPI, indexDefinition );
        }
        Node myNode;
        {
            try ( Transaction tx = beansAPI.beginTx() )
            {
                myNode = beansAPI.getNodeById( id );
                myNode.addLabel( LABEL1 );
                myNode.setProperty( "key2", LONG_STRING );
                myNode.setProperty( "key3", LONG_STRING );

                tx.success();
            }
        }

        // Then
        assertThat( myNode, inTx( beansAPI, hasProperty( "key2" ).withValue( LONG_STRING ) ) );
        assertThat( myNode, inTx( beansAPI, hasProperty( "key3" ).withValue( LONG_STRING ) ) );
        assertThat( findNodesByLabelAndProperty( LABEL1, "key2", LONG_STRING, beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingForNodeByPropertyShouldWorkWithoutIndex() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), LABEL1 );

        // When
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingUsesIndexWhenItExists() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), LABEL1 );
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );

        // When
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyAtTheSameTime() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), LABEL1, LABEL2 );
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );
        Neo4jMatchers.createIndex( beansAPI, LABEL2, "name" );
        Neo4jMatchers.createIndex( beansAPI, LABEL3, "name" );

        // When
        try ( Transaction tx = beansAPI.beginTx() )
        {
            myNode.removeLabel( LABEL1 );
            myNode.addLabel( LABEL3 );
            myNode.setProperty( "name", "Einstein" );
            tx.success();
        }

        // Then
        assertThat( myNode, inTx( beansAPI, hasProperty("name").withValue( "Einstein" ) ) );
        assertThat( labels( myNode ), containsOnly( LABEL2, LABEL3 ) );

        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", beansAPI ), isEmpty() );

        assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Einstein", beansAPI ), containsOnly( myNode ) );

        assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Einstein", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyMultipleTimesAllAtOnce() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), LABEL1, LABEL2 );
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );
        Neo4jMatchers.createIndex( beansAPI, LABEL2, "name" );
        Neo4jMatchers.createIndex( beansAPI, LABEL3, "name" );

        // When
        try ( Transaction tx = beansAPI.beginTx() )
        {
            myNode.addLabel( LABEL3 );
            myNode.setProperty( "name", "Einstein" );
            myNode.removeLabel( LABEL1 );
            myNode.setProperty( "name", "Feynman" );
            tx.success();
        }

        // Then
        assertThat( myNode, inTx( beansAPI, hasProperty("name").withValue( "Feynman" ) ) );
        assertThat( labels( myNode ), containsOnly( LABEL2, LABEL3 ) );

        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Feynman", beansAPI ), isEmpty() );

        assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Feynman", beansAPI ), containsOnly( myNode ) );

        assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Hawking", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", beansAPI ), isEmpty() );
        assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Feynman", beansAPI ), containsOnly( myNode ) );
    }

    @Test
    public void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When/Then
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", beansAPI ), isEmpty() );
    }

    @Test
    public void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), LABEL1 );

        // WHEN THEN
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Mattias", beansAPI ), containsOnly( firstNode ) );
        Node secondNode = createNode( beansAPI, map( "name", "Taylor" ), LABEL1 );
        assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Taylor", beansAPI ), containsOnly( secondNode ) );
    }

    @Test
    public void createdNodeShouldShowUpWithinTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), LABEL1 );
        long sizeBeforeDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );

        tx.close();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(0l) );
    }

    @Test
    public void deletedNodeShouldShowUpWithinTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), LABEL1 );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );
        firstNode.delete();
        long sizeAfterDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );

        tx.close();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(0l) );
    }

    @Test
    public void createdNodeShouldShowUpInIndexQuery() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( beansAPI, LABEL1, "name" );
        createNode( beansAPI, map( "name", "Mattias" ), LABEL1 );

        // WHEN
        Transaction tx = beansAPI.beginTx();

        long sizeBeforeDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );
        createNode( beansAPI, map( "name", "Mattias" ), LABEL1 );
        long sizeAfterDelete = count( beansAPI.findNodes( LABEL1, "name", "Mattias" ) );

        tx.close();

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1l) );
        assertThat( sizeAfterDelete, equalTo(2l) );
    }

    @Test
    public void shouldBeAbleToQuerySupportedPropertyTypes() throws Exception
    {
        // GIVEN
        String property = "name";
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( db, LABEL1, property );

        // WHEN & THEN
        assertCanCreateAndFind( db, LABEL1, property, "A String" );
        assertCanCreateAndFind( db, LABEL1, property, true );
        assertCanCreateAndFind( db, LABEL1, property, false );
        assertCanCreateAndFind( db, LABEL1, property, (byte) 56 );
        assertCanCreateAndFind( db, LABEL1, property, 'z' );
        assertCanCreateAndFind( db, LABEL1, property, (short)12 );
        assertCanCreateAndFind( db, LABEL1, property, 12 );
        assertCanCreateAndFind( db, LABEL1, property, 12l );
        assertCanCreateAndFind( db, LABEL1, property, (float)12. );
        assertCanCreateAndFind( db, LABEL1, property, 12. );

        assertCanCreateAndFind( db, LABEL1, property, new String[]{"A String"} );
        assertCanCreateAndFind( db, LABEL1, property, new boolean[]{true} );
        assertCanCreateAndFind( db, LABEL1, property, new Boolean[]{false} );
        assertCanCreateAndFind( db, LABEL1, property, new byte[]{56} );
        assertCanCreateAndFind( db, LABEL1, property, new Byte[]{57} );
        assertCanCreateAndFind( db, LABEL1, property, new char[]{'a'} );
        assertCanCreateAndFind( db, LABEL1, property, new Character[]{'b'} );
        assertCanCreateAndFind( db, LABEL1, property, new short[]{12} );
        assertCanCreateAndFind( db, LABEL1, property, new Short[]{13} );
        assertCanCreateAndFind( db, LABEL1, property, new int[]{14} );
        assertCanCreateAndFind( db, LABEL1, property, new Integer[]{15} );
        assertCanCreateAndFind( db, LABEL1, property, new long[]{16l} );
        assertCanCreateAndFind( db, LABEL1, property, new Long[]{17l} );
        assertCanCreateAndFind( db, LABEL1, property, new float[]{(float)18.} );
        assertCanCreateAndFind( db, LABEL1, property, new Float[]{(float)19.} );
        assertCanCreateAndFind( db, LABEL1, property, new double[]{20.} );
        assertCanCreateAndFind( db, LABEL1, property, new Double[]{21.} );
    }

    @Test
    public void shouldRetrieveMultipleNodesWithSameValueFromIndex() throws Exception
    {
        // this test was included here for now as a precondition for the following test

        // given
        GraphDatabaseService graph = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( graph, LABEL1, "name" );

        Node node1, node2;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode( LABEL1 );
            node1.setProperty( "name", "Stefan" );

            node2 = graph.createNode( LABEL1 );
            node2.setProperty( "name", "Stefan" );
            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            ResourceIterator<Node> result = graph.findNodes( LABEL1, "name", "Stefan" );
            assertEquals( asSet( node1, node2 ), asSet( result ) );

            tx.success();
        }
    }

    @Test( expected = MultipleFoundException.class )
    public void shouldThrowWhenMulitpleResultsForSingleNode() throws Exception
    {
        // given
        GraphDatabaseService graph = dbRule.getGraphDatabaseService();
        Neo4jMatchers.createIndex( graph, LABEL1, "name" );

        Node node1, node2;
        try ( Transaction tx = graph.beginTx() )
        {
            node1 = graph.createNode( LABEL1 );
            node1.setProperty( "name", "Stefan" );

            node2 = graph.createNode( LABEL1 );
            node2.setProperty( "name", "Stefan" );
            tx.success();
        }

        try ( Transaction tx = graph.beginTx() )
        {
            graph.findNode( LABEL1, "name", "Stefan" );
        }
    }

    @Test
    public void shouldAddIndexedPropertyToNodeWithDynamicLabels()
    {
        // Given
        int indexesCount = 20;
        String labelPrefix = "foo";
        String propertyKeyPrefix = "bar";
        String propertyValuePrefix = "baz";
        GraphDatabaseService db = dbRule.getGraphDatabaseService();

        for ( int i = 0; i < indexesCount; i++ )
        {
            Neo4jMatchers.createIndex( db, DynamicLabel.label( labelPrefix + i ), propertyKeyPrefix + i );
        }

        // When
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode().getId();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            for ( int i = 0; i < indexesCount; i++ )
            {
                node.addLabel( DynamicLabel.label( labelPrefix + i ) );
                node.setProperty( propertyKeyPrefix + i, propertyValuePrefix + i );
            }
            tx.success();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < indexesCount; i++ )
            {
                Label label = DynamicLabel.label( labelPrefix + i );
                String key = propertyKeyPrefix + i;
                String value = propertyValuePrefix + i;

                ResourceIterable<Node> nodes = db.findNodesByLabelAndProperty( label, key, value );
                assertEquals( 1, Iterables.count( nodes ) );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSupportIndexSeekByPrefix()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        IndexDefinition index = createIndex( db, LABEL1, "name" );
        createNodes( db, LABEL1, "name", "Mattias", "Mats", "Carla" );
        PrimitiveLongSet expected = createNodes( db, LABEL1, "name", "Karl", "Karlsson" );

        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            Statement statement = getStatement( (GraphDatabaseAPI) db );
            ReadOperations ops = statement.readOperations();
            IndexDescriptor descriptor = indexDescriptor( ops, index );
            found.addAll( ops.nodesGetFromIndexRangeSeekByPrefix( descriptor, "Karl" ) );
        }

        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldIncludeNodesCreatedInSameTxInIndexSeekByPrefix()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        IndexDefinition index = createIndex( db, LABEL1, "name" );
        createNodes( db, LABEL1, "name", "Mattias", "Mats" );
        PrimitiveLongSet expected = createNodes( db, LABEL1, "name", "Karl", "Karlsson" );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            expected.add( createNode( db, map( "name", "Karlchen" ), LABEL1 ).getId() );
            createNode( db, map( "name", "Carla" ), LABEL1 );
            Statement statement = getStatement( (GraphDatabaseAPI) db );
            ReadOperations readOperations = statement.readOperations();
            IndexDescriptor descriptor = indexDescriptor( readOperations, index );
            found.addAll( readOperations.nodesGetFromIndexRangeSeekByPrefix( descriptor, "Karl" ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldNotIncludeNodesDeletedInSameTxInIndexSeekByPrefix()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        IndexDefinition index = createIndex( db, LABEL1, "name" );
        createNodes( db, LABEL1, "name", "Mattias" );
        PrimitiveLongSet toDelete = createNodes( db, LABEL1, "name", "Karlsson", "Mats" );
        PrimitiveLongSet expected = createNodes( db, LABEL1, "name", "Karl" );
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator deleting = toDelete.iterator();
            while ( deleting.hasNext() )
            {
                long id = deleting.next();
                db.getNodeById( id ).delete();
                expected.remove( id );
            }
            Statement statement = getStatement( (GraphDatabaseAPI) db );
            ReadOperations readOperations = statement.readOperations();
            IndexDescriptor descriptor = indexDescriptor( readOperations, index );
            found.addAll( readOperations.nodesGetFromIndexRangeSeekByPrefix( descriptor, "Karl" ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    @Test
    public void shouldConsiderNodesChangedInSameTxInIndexPrefixSearch()
            throws SchemaRuleNotFoundException, IndexNotFoundKernelException
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        IndexDefinition index = createIndex( db, LABEL1, "name" );
        createNodes( db, LABEL1, "name", "Mattias" );
        PrimitiveLongSet toChangeToMatch = createNodes( db, LABEL1, "name", "Mats" );
        PrimitiveLongSet toChangeToNotMatch = createNodes( db, LABEL1, "name", "Karlsson" );
        PrimitiveLongSet expected = createNodes( db, LABEL1, "name", "Karl" );
        String prefix = "Karl";
        // WHEN
        PrimitiveLongSet found = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator toMatching = toChangeToMatch.iterator();
            while ( toMatching.hasNext() )
            {
                long id = toMatching.next();
                db.getNodeById( id ).setProperty( "name", prefix + "X" + id );
                expected.add( id );
            }
            PrimitiveLongIterator toNotMatching = toChangeToNotMatch.iterator();
            while ( toNotMatching.hasNext() )
            {
                long id = toNotMatching.next();
                db.getNodeById( id ).setProperty( "name", "X" + id );
                expected.remove( id );
            }
            Statement statement = getStatement( (GraphDatabaseAPI) db );
            ReadOperations readOperations = statement.readOperations();
            IndexDescriptor descriptor = indexDescriptor( readOperations, index );
            found.addAll( readOperations.nodesGetFromIndexRangeSeekByPrefix( descriptor, prefix ) );
        }
        // THEN
        assertThat( found, equalTo( expected ) );
    }

    private IndexDefinition createIndex( GraphDatabaseService db, Label label, String propertyKey )
    {
        IndexDefinition index = null;
        try ( Transaction tx = db.beginTx() )
        {
            // create index
            index = db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, TimeUnit.SECONDS );
            tx.success();
        }
        return index;
    }

    private PrimitiveLongSet createNodes( GraphDatabaseService db, Label label, String propertyKey, String... propertyValues )
    {
        PrimitiveLongSet expected = Primitive.longSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( String value : propertyValues )
            {
                expected.add( createNode( db, map( propertyKey, value ), label ).getId() );
            }
            tx.success();
        }
        return expected;
    }

    private IndexDescriptor indexDescriptor(ReadOperations readOperations, IndexDefinition index)
            throws SchemaRuleNotFoundException
    {
        int labelId = readOperations.labelGetForName( index.getLabel().name() );
        String propertyName = index.getPropertyKeys().iterator().next();
        int propertyId = readOperations.propertyKeyGetForName( propertyName );
        return readOperations.indexesGetForLabelAndPropertyKey( labelId, propertyId );
    }

    private Statement getStatement( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).get();
    }

    private void assertCanCreateAndFind( GraphDatabaseService db, Label label, String propertyKey, Object value )
    {
        Node created = createNode( db, map( propertyKey, value ), label );

        try ( Transaction tx = db.beginTx() )
        {
            Node found = db.findNode( label, propertyKey, value );
            assertThat( found, equalTo( created ) );
            found.delete();
            tx.success();
        }
    }

    public static final String LONG_STRING = "a long string that has to be stored in dynamic records";

    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private Label LABEL1 = DynamicLabel.label( "LABEL1" );
    private Label LABEL2 = DynamicLabel.label( "LABEL2" );
    private Label LABEL3 = DynamicLabel.label( "LABEL3" );

    private Node createNode( GraphDatabaseService beansAPI, Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = beansAPI.beginTx() )
        {
            Node node = beansAPI.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
                node.setProperty( property.getKey(), property.getValue() );
            tx.success();
            return node;
        }
    }

    private Neo4jMatchers.Deferred<Label> labels( final Node myNode )
    {
        return new Neo4jMatchers.Deferred<Label>( dbRule.getGraphDatabaseService() )
        {
            @Override
            protected Iterable<Label> manifest()
            {
                return myNode.getLabels();
            }
        };
    }
}
