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
package org.neo4j.graphdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Map;

import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.mockito.matcher.Neo4jMatchers;
import org.neo4j.test.mockito.mock.SpatialMocks;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.isEmpty;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockCartesian_3D;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84;
import static org.neo4j.test.mockito.mock.SpatialMocks.mockWGS84_3D;

@DbmsExtension
class IndexingAcceptanceTest
{
    private static final String LONG_STRING = "a long string that has to be stored in dynamic records";

    @Inject
    private GraphDatabaseAPI db;

    private Label LABEL1;
    private Label LABEL2;
    private Label LABEL3;

    @BeforeEach
    void setupLabels( TestInfo testInfo )
    {
        LABEL1 = Label.label( "LABEL1-" + testInfo.getDisplayName() );
        LABEL2 = Label.label( "LABEL2-" + testInfo.getDisplayName() );
        LABEL3 = Label.label( "LABEL3-" + testInfo.getDisplayName() );
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
    void shouldInterpretPropertyAsChangedEvenIfPropertyMovesFromOneRecordToAnother()
    {
        // GIVEN
        long smallValue = 10L;
        long bigValue = 1L << 62;
        Node myNode;
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.createNode( LABEL1 );
            myNode.setProperty( "pad0", true );
            myNode.setProperty( "pad1", true );
            myNode.setProperty( "pad2", true );
            // Use a small long here which will only occupy one property block
            myNode.setProperty( "key", smallValue );

            tx.commit();
        }

        Neo4jMatchers.createIndex( db, LABEL1, "key" );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            // A big long value which will occupy two property blocks
            tx.getNodeById( myNode.getId() ).setProperty( "key", bigValue );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            // THEN
            assertThat( findNodesByLabelAndProperty( LABEL1, "key", bigValue, db, transaction ), containsOnly( myNode ) );
            assertThat( findNodesByLabelAndProperty( LABEL1, "key", smallValue, db, transaction ), isEmpty() );
        }
    }

    @Test
    void shouldUseDynamicPropertiesToIndexANodeWhenAddedAlongsideExistingPropertiesInASeparateTransaction()
    {
        // When
        long id;
        try ( Transaction tx = db.beginTx() )
        {
            Node myNode = tx.createNode();
            id = myNode.getId();
            myNode.setProperty( "key0", true );
            myNode.setProperty( "key1", true );

            tx.commit();
        }

        Neo4jMatchers.createIndex( db, LABEL1, "key2" );
        Node myNode;
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.getNodeById( id );
            myNode.addLabel( LABEL1 );
            myNode.setProperty( "key2", LONG_STRING );
            myNode.setProperty( "key3", LONG_STRING );

            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            myNode = transaction.getNodeById( myNode.getId() );
            // Then
            assertThat( myNode, hasProperty( "key2" ).withValue( LONG_STRING ) );
            assertThat( myNode, hasProperty( "key3" ).withValue( LONG_STRING ) );
            assertThat( findNodesByLabelAndProperty( LABEL1, "key2", LONG_STRING, db, transaction ), containsOnly( myNode ) );
        }
    }

    @Test
    void searchingForNodeByPropertyShouldWorkWithoutIndex()
    {
        // Given
        Node myNode = createNode( db, map( "name", "Hawking" ), LABEL1 );

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", db, transaction ), containsOnly( myNode ) );
        }
    }

    @Test
    void searchingUsesIndexWhenItExists()
    {
        // Given
        Node myNode = createNode( db, map( "name", "Hawking" ), LABEL1 );
        Neo4jMatchers.createIndex( db, LABEL1, "name" );

        // When
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", db, transaction ), containsOnly( myNode ) );
        }
    }

    @Test
    void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyAtTheSameTime()
    {
        // Given
        Node myNode = createNode( db, map( "name", "Hawking" ), LABEL1, LABEL2 );
        Neo4jMatchers.createIndex( db, LABEL1, "name" );
        Neo4jMatchers.createIndex( db, LABEL2, "name" );
        Neo4jMatchers.createIndex( db, LABEL3, "name" );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.getNodeById( myNode.getId() );
            myNode.removeLabel( LABEL1 );
            myNode.addLabel( LABEL3 );
            myNode.setProperty( "name", "Einstein" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            myNode = transaction.getNodeById( myNode.getId() );
            // Then
            assertThat( myNode, hasProperty( "name" ).withValue( "Einstein" ) );
            assertThat( labels( myNode ), containsOnly( LABEL2, LABEL3 ) );

            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", db, transaction ), isEmpty() );

            assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Einstein", db, transaction ), containsOnly( myNode ) );

            assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Einstein", db, transaction ), containsOnly( myNode ) );
            transaction.commit();
        }
    }

    @Test
    void shouldCorrectlyUpdateIndexesWhenChangingLabelsAndPropertyMultipleTimesAllAtOnce()
    {
        // Given
        Node myNode = createNode( db, map( "name", "Hawking" ), LABEL1, LABEL2 );
        Neo4jMatchers.createIndex( db, LABEL1, "name" );
        Neo4jMatchers.createIndex( db, LABEL2, "name" );
        Neo4jMatchers.createIndex( db, LABEL3, "name" );

        // When
        try ( Transaction tx = db.beginTx() )
        {
            myNode = tx.getNodeById( myNode.getId() );
            myNode.addLabel( LABEL3 );
            myNode.setProperty( "name", "Einstein" );
            myNode.removeLabel( LABEL1 );
            myNode.setProperty( "name", "Feynman" );
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            myNode = transaction.getNodeById( myNode.getId() );
            // Then
            assertThat( myNode, hasProperty( "name" ).withValue( "Feynman" ) );
            assertThat( labels( myNode ), containsOnly( LABEL2, LABEL3 ) );

            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Einstein", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Feynman", db, transaction ), isEmpty() );

            assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Einstein", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL2, "name", "Feynman", db, transaction ), containsOnly( myNode ) );

            assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Hawking", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Einstein", db, transaction ), isEmpty() );
            assertThat( findNodesByLabelAndProperty( LABEL3, "name", "Feynman", db, transaction ), containsOnly( myNode ) );
            transaction.commit();
        }
    }

    @Test
    void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty()
    {
        // When/Then
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Hawking", db, transaction ), isEmpty() );
        }
    }

    @Test
    void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction()
    {
        // GIVEN
        Neo4jMatchers.createIndex( db, LABEL1, "name" );
        Node firstNode = createNode( db, map( "name", "Mattias" ), LABEL1 );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Mattias", db, transaction ), containsOnly( firstNode ) );
        }
        Node secondNode = createNode( db, map( "name", "Taylor" ), LABEL1 );
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( LABEL1, "name", "Taylor", db, transaction ), containsOnly( secondNode ) );
        }
    }

    @Test
    void createdNodeShouldShowUpWithinTransaction()
    {
        // GIVEN
        Neo4jMatchers.createIndex( db, LABEL1, "name" );

        // WHEN

        Node firstNode = createNode( db, map( "name", "Mattias" ), LABEL1 );
        long sizeBeforeDelete;
        long sizeAfterDelete;
        try ( Transaction tx = db.beginTx() )
        {
            sizeBeforeDelete = count( tx.findNodes( LABEL1, "name", "Mattias" ) );
            tx.getNodeById( firstNode.getId() ).delete();
            sizeAfterDelete = count( tx.findNodes( LABEL1, "name", "Mattias" ) );
            tx.commit();
        }

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1L) );
        assertThat( sizeAfterDelete, equalTo(0L) );
    }

    @Test
    void deletedNodeShouldShowUpWithinTransaction()
    {
        // GIVEN
        Neo4jMatchers.createIndex( db, LABEL1, "name" );
        Node firstNode = createNode( db, map( "name", "Mattias" ), LABEL1 );

        // WHEN
        long sizeBeforeDelete;
        long sizeAfterDelete;
        try ( Transaction tx = db.beginTx() )
        {
            sizeBeforeDelete = count( tx.findNodes( LABEL1, "name", "Mattias" ) );
            tx.getNodeById( firstNode.getId() ).delete();
            sizeAfterDelete = count( tx.findNodes( LABEL1, "name", "Mattias" ) );
            tx.commit();
        }

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1L) );
        assertThat( sizeAfterDelete, equalTo(0L) );
    }

    @Test
    void createdNodeShouldShowUpInIndexQuery()
    {
        // GIVEN
        Neo4jMatchers.createIndex( db, LABEL1, "name" );
        createNode( db, map( "name", "Mattias" ), LABEL1 );

        // WHEN
        long sizeBeforeDelete;
        long sizeAfterDelete;
        try ( Transaction transaction = db.beginTx() )
        {
            sizeBeforeDelete = count( transaction.findNodes( LABEL1, "name", "Mattias" ) );
        }
        createNode( db, map( "name", "Mattias" ), LABEL1 );
        try ( Transaction transaction = db.beginTx() )
        {
            sizeAfterDelete = count( transaction.findNodes( LABEL1, "name", "Mattias" ) );
        }

        // THEN
        assertThat( sizeBeforeDelete, equalTo(1L) );
        assertThat( sizeAfterDelete, equalTo(2L) );
    }

    @Test
    void shouldBeAbleToQuerySupportedPropertyTypes()
    {
        // GIVEN
        String property = "name";
        Neo4jMatchers.createIndex( db, LABEL1, property );

        // WHEN & THEN
        assertCanCreateAndFind( db, LABEL1, property, "A String" );
        assertCanCreateAndFind( db, LABEL1, property, true );
        assertCanCreateAndFind( db, LABEL1, property, false );
        assertCanCreateAndFind( db, LABEL1, property, (byte) 56 );
        assertCanCreateAndFind( db, LABEL1, property, 'z' );
        assertCanCreateAndFind( db, LABEL1, property, (short)12 );
        assertCanCreateAndFind( db, LABEL1, property, 12 );
        assertCanCreateAndFind( db, LABEL1, property, 12L );
        assertCanCreateAndFind( db, LABEL1, property, (float)12. );
        assertCanCreateAndFind( db, LABEL1, property, 12. );
        assertCanCreateAndFind( db, LABEL1, property, SpatialMocks.mockPoint( 12.3, 45.6, mockWGS84() ) );
        assertCanCreateAndFind( db, LABEL1, property, SpatialMocks.mockPoint( 123, 456, mockCartesian() ) );
        assertCanCreateAndFind( db, LABEL1, property, SpatialMocks.mockPoint( 12.3, 45.6, 100.0, mockWGS84_3D() ) );
        assertCanCreateAndFind( db, LABEL1, property, SpatialMocks.mockPoint( 123, 456, 789, mockCartesian_3D() ) );
        assertCanCreateAndFind( db, LABEL1, property, Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 ) );
        assertCanCreateAndFind( db, LABEL1, property, Values.pointValue( CoordinateReferenceSystem.Cartesian, 123, 456 ) );
        assertCanCreateAndFind( db, LABEL1, property, Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.3, 45.6, 100.0 ) );
        assertCanCreateAndFind( db, LABEL1, property, Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 123, 456, 789 ) );

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
        assertCanCreateAndFind( db, LABEL1, property, new long[]{16L} );
        assertCanCreateAndFind( db, LABEL1, property, new Long[]{17L} );
        assertCanCreateAndFind( db, LABEL1, property, new float[]{(float)18.} );
        assertCanCreateAndFind( db, LABEL1, property, new Float[]{(float)19.} );
        assertCanCreateAndFind( db, LABEL1, property, new double[]{20.} );
        assertCanCreateAndFind( db, LABEL1, property, new Double[]{21.} );
        assertCanCreateAndFind( db, LABEL1, property, new Point[]{SpatialMocks.mockPoint( 12.3, 45.6, mockWGS84() )} );
        assertCanCreateAndFind( db, LABEL1, property, new Point[]{SpatialMocks.mockPoint( 123, 456, mockCartesian() )} );
        assertCanCreateAndFind( db, LABEL1, property, new Point[]{SpatialMocks.mockPoint( 12.3, 45.6, 100.0, mockWGS84_3D() )} );
        assertCanCreateAndFind( db, LABEL1, property, new Point[]{SpatialMocks.mockPoint( 123, 456, 789, mockCartesian_3D() )} );
        assertCanCreateAndFind( db, LABEL1, property, new PointValue[]{Values.pointValue( CoordinateReferenceSystem.WGS84, 12.3, 45.6 )} );
        assertCanCreateAndFind( db, LABEL1, property, new PointValue[]{Values.pointValue( CoordinateReferenceSystem.Cartesian, 123, 456 )} );
        assertCanCreateAndFind( db, LABEL1, property, new PointValue[]{Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 12.3, 45.6, 100.0 )} );
        assertCanCreateAndFind( db, LABEL1, property, new PointValue[]{Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 123, 456, 789 )} );
    }

    @Test
    void shouldRetrieveMultipleNodesWithSameValueFromIndex()
    {
        // this test was included here for now as a precondition for the following test

        // given
        Neo4jMatchers.createIndex( db, LABEL1, "name" );

        Node node1;
        Node node2;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.createNode( LABEL1 );
            node1.setProperty( "name", "Stefan" );

            node2 = tx.createNode( LABEL1 );
            node2.setProperty( "name", "Stefan" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            ResourceIterator<Node> result = tx.findNodes( LABEL1, "name", "Stefan" );
            assertEquals( asSet( node1, node2 ), asSet( result ) );

            tx.commit();
        }
    }

    @Test
    void shouldThrowWhenMultipleResultsForSingleNode()
    {
        // given
        Neo4jMatchers.createIndex( db, LABEL1, "name" );

        Node node1;
        Node node2;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.createNode( LABEL1 );
            node1.setProperty( "name", "Stefan" );

            node2 = tx.createNode( LABEL1 );
            node2.setProperty( "name", "Stefan" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            var e = assertThrows( MultipleFoundException.class, () -> tx.findNode( LABEL1, "name", "Stefan" ) );
            assertThat( e.getMessage(), equalTo(
                    format( "Found multiple nodes with label: '%s', property name: 'name' " +
                            "and property value: 'Stefan' while only one was expected.", LABEL1 ) ) );
        }
    }

    @Test
    void shouldAddIndexedPropertyToNodeWithDynamicLabels()
    {
        // Given
        int indexesCount = 20;
        String labelPrefix = "foo";
        String propertyKeyPrefix = "bar";
        String propertyValuePrefix = "baz";

        for ( int i = 0; i < indexesCount; i++ )
        {
            Neo4jMatchers.createIndexNoWait( db, Label.label( labelPrefix + i ), propertyKeyPrefix + i );
        }
        Neo4jMatchers.waitForIndexes( db );

        // When
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = tx.createNode().getId();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            for ( int i = 0; i < indexesCount; i++ )
            {
                node.addLabel( Label.label( labelPrefix + i ) );
                node.setProperty( propertyKeyPrefix + i, propertyValuePrefix + i );
            }
            tx.commit();
        }

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < indexesCount; i++ )
            {
                Label label = Label.label( labelPrefix + i );
                String key = propertyKeyPrefix + i;
                String value = propertyValuePrefix + i;

                ResourceIterator<Node> nodes = tx.findNodes( label, key, value );
                assertEquals( 1, Iterators.count( nodes ) );
            }
            tx.commit();
        }
    }

    private void assertCanCreateAndFind( GraphDatabaseService db, Label label, String propertyKey, Object value )
    {
        Node created = createNode( db, map( propertyKey, value ), label );

        try ( Transaction tx = db.beginTx() )
        {
            Node found = tx.findNode( label, propertyKey, value );
            assertThat( found, equalTo( created ) );
            found.delete();
            tx.commit();
        }
    }

    private Node createNode( GraphDatabaseService db, Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.commit();
            return node;
        }
    }

    private Neo4jMatchers.Deferred<Label> labels( final Node myNode )
    {
        return new Neo4jMatchers.Deferred<>()
        {
            @Override
            protected Iterable<Label> manifest()
            {
                return myNode.getLabels();
            }
        };
    }
}
