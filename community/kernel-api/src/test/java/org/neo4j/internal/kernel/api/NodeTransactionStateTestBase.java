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
package org.neo4j.internal.kernel.api;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class NodeTransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldSeeNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );
                assertEquals( nodeId, node.nodeReference() );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertEquals( nodeId, graphDb.getNodeById( nodeId ).getId() );
        }
    }

    @Test
    public void shouldSeeNewLabeledNodeInTransaction() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            labelId = session.token().labelGetOrCreateForName( labelName );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                LabelSet labels = node.labels();
                assertEquals( 1, labels.numberOfLabels() );
                assertEquals( labelId, labels.label( 0 ) );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    equalTo( Iterables.iterable( label( labelName ) ) ) );
        }
    }

    @Test
    public void shouldSeeLabelChangesInTransaction() throws Exception
    {
        long nodeId;
        int toRetain, toDelete, toAdd;
        final String toRetainName = "ToRetain";
        final String toDeleteName = "ToDelete";
        final String toAddName = "ToAdd";

        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            toRetain = session.token().labelGetOrCreateForName( toRetainName );
            toDelete = session.token().labelGetOrCreateForName( toDeleteName );
            tx.dataWrite().nodeAddLabel( nodeId, toRetain );
            tx.dataWrite().nodeAddLabel( nodeId, toDelete );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    containsInAnyOrder( label( toRetainName ), label( toDeleteName ) ) );
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            toAdd = session.token().labelGetOrCreateForName( toAddName );
            tx.dataWrite().nodeAddLabel( nodeId, toAdd );
            tx.dataWrite().nodeRemoveLabel( nodeId, toDelete );

            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                assertLabels( node.labels(), toRetain, toAdd );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getLabels(),
                    containsInAnyOrder( label( toRetainName ), label( toAddName ) ) );
        }
    }

    @Test
    public void shouldDiscoverDeletedNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            try ( NodeCursor node = cursors.allocateNodeCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertFalse( node.next() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldHandleMultipleNodeDeletions() throws Exception
    {
        long nodeId;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            assertFalse( tx.dataWrite().nodeDelete( nodeId ) );
            tx.success();
        }
    }

    @Test
    public void shouldSeeNewNodePropertyInTransaction() throws Exception
    {
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";

        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            int prop1 = session.token().propertyKeyGetOrCreateForName( propKey1 );
            int prop2 = session.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, prop1, stringValue( "hello" ) ), NO_VALUE );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, prop2, stringValue( "world" ) ), NO_VALUE );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );
                assertTrue( property.next() );
                //First property
                assertEquals( prop1, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "hello" ) );
                //second property
                assertTrue( property.next() );
                assertEquals( prop2, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                assertFalse( property.next(), "should only find two properties" );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeAddedPropertyFromExistingNodeWithoutPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            int propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );
                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "hello" ) );

                assertFalse( property.next(), "should only find one properties" );
                assertFalse( node.next(), "should only find one node" );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getProperty( propKey ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldSeeAddedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";
        int propToken1;
        int propToken2;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken1 = session.token().propertyKeyGetOrCreateForName( propKey1 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken1, stringValue( "hello" ) ), NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            propToken2 = session.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken2, stringValue( "world" ) ), NO_VALUE );

            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );

                //property 2, start with tx state
                assertTrue( property.next() );
                assertEquals( propToken2, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                //property 1, from disk
                assertTrue( property.next() );
                assertEquals( propToken1, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "hello" ) );

                assertFalse( property.next(), "should only find two properties" );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getProperty( propKey1 ), equalTo( "hello" ) );
            assertThat(
                    graphDb.getNodeById( nodeId ).getProperty( propKey2 ), equalTo( "world" ) );
        }
    }

    @Test
    public void shouldSeeUpdatedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "world" ) ),
                    stringValue( "hello" ) );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );

                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                assertFalse( property.next(), "should only find one property" );
                assertFalse( node.next(), "should only find one node" );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getProperty( propKey ), equalTo( "world" ) );
        }
    }

    @Test
    public void shouldSeeRemovedPropertyInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeRemoveProperty( nodeId, propToken ), stringValue( "hello" ) );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );
                assertFalse( property.next(), "should not find any properties" );
                assertFalse( node.next(), "should only find one node" );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertFalse(
                    graphDb.getNodeById( nodeId ).hasProperty( propKey ) );
        }
    }

    @Test
    public void shouldSeeRemovedThenAddedPropertyInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( Transaction tx = session.beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = session.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.success();
        }

        // When/Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeRemoveProperty( nodeId, propToken ), stringValue( "hello" ) );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "world" ) ), NO_VALUE );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  PropertyCursor property = cursors.allocatePropertyCursor() )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );
                assertTrue( property.next() );
                assertEquals( propToken, property.propertyKey() );
                assertEquals( property.propertyValue(), stringValue( "world" ) );

                assertFalse( property.next(), "should not find any properties" );
                assertFalse( node.next(), "should only find one node" );
            }

            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignored = graphDb.beginTx() )
        {
            assertThat(
                    graphDb.getNodeById( nodeId ).getProperty( propKey ), equalTo( "world" ) );
        }
    }

    @Test
    public void shouldSeeExistingNode() throws Exception
    {
        // Given
        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.success();
        }

        // Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataRead().nodeExists( node ) );
        }
    }

    @Test
    public void shouldNotSeeNonExistingNode() throws Exception
    {
        // Given, empty db

        // Then
        try ( Transaction tx = session.beginTransaction() )
        {
            assertFalse( tx.dataRead().nodeExists( 1337L ) );
        }
    }

    @Test
    public void shouldSeeNodeExistingInTxOnly() throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            long node = tx.dataWrite().nodeCreate();
            assertTrue( tx.dataRead().nodeExists( node ) );

        }
    }

    @Test
    public void shouldNotSeeDeletedNode() throws Exception
    {
        // Given
        long node;
        try ( Transaction tx = session.beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.success();
        }

        // Then
        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().nodeDelete( node );
            assertFalse( tx.dataRead().nodeExists( node ) );
        }
    }

    @Test
    public void shouldNotFindDeletedNodeInLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( node.node );
            tx.dataRead().nodeLabelScan( node.labels[0], cursor );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotFindNodeWithRemovedLabelInLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[0] );
            tx.dataRead().nodeLabelScan( node.labels[0], cursor );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldFindUpdatedNodeInInLabelScan() throws Exception
    {
        // Given
        Node node = createNode(  );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            int label = tx.tokenWrite().labelGetOrCreateForName( "label" );
            tx.dataWrite().nodeAddLabel( node.node, label );
            tx.dataRead().nodeLabelScan( label, cursor );

            // then
            assertTrue( cursor.next() );
            assertEquals( node.node, cursor.nodeReference() );
        }
    }

    @Test
    public void shouldFindSwappedNodeInLabelScan() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode();

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node1.node, node1.labels[0] );
            tx.dataWrite().nodeAddLabel( node2.node, node1.labels[0] );
            tx.dataRead().nodeLabelScan( node1.labels[0], cursor );

            // then
            assertTrue( cursor.next() );
            assertEquals( node2.node, cursor.nodeReference() );
        }
    }

    @Ignore
    public void shouldNotFindDeletedNodeInDisjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1", "label2" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( node.node );
            tx.dataRead().nodeLabelUnionScan( cursor, node.labels );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Ignore
    public void shouldFindNodeWithOneRemovedLabelInDisjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1", "label2" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[1] );
            tx.dataRead().nodeLabelUnionScan( cursor, node.labels );

            // then
            assertTrue( cursor.next() );
            assertEquals( node.node, cursor.nodeReference() );
        }
    }

    @Ignore
    public void shouldNotFindNodeWithAllRemovedLabelsInDisjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1", "label2" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[0] );
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[1] );
            tx.dataRead().nodeLabelUnionScan( cursor, node.labels );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Ignore
    public void shouldNotFindNodeWithOneRemovedLabelsInDisjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1");

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            int label1 = tx.tokenWrite().labelGetOrCreateForName( "label1" );
            int label2 = tx.tokenWrite().labelGetOrCreateForName( "label2" );

            tx.dataWrite().nodeRemoveLabel( node.node, label1 );
            tx.dataRead().nodeLabelUnionScan( cursor, label1, label2 );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Ignore
    public void shouldFindUpdatedNodeInInDisjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            int label2 = tx.tokenWrite().labelGetOrCreateForName( "label2" );
            tx.dataWrite().nodeAddLabel( node.node, label2 );
            tx.dataRead().nodeLabelUnionScan( cursor, node.labels[0], label2 );

            // then
            assertTrue( cursor.next() );
            assertEquals( node.node, cursor.nodeReference() );
        }
    }

    @Ignore
    public void shouldNotFindDeletedNodeInConjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1", "label2" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( node.node );
            tx.dataRead().nodeLabelIntersectionScan( cursor, node.labels );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Ignore
    public void shouldNotFindNodeWithRemovedLabelInConjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label1", "label2" );

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[1] );
            tx.dataRead().nodeLabelIntersectionScan( cursor, node.labels );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Ignore
    public void shouldFindUpdatedNodeInInConjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode("label1");

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            int label2 = tx.tokenWrite().labelGetOrCreateForName( "label2" );
            tx.dataWrite().nodeAddLabel( node.node, label2 );
            tx.dataRead().nodeLabelIntersectionScan( cursor, node.labels[0], label2 );

            // then
            assertTrue( cursor.next() );
            assertEquals( node.node, cursor.nodeReference() );
        }
    }

    @Ignore
    public void shouldNotFindNodeWithJustOneUpdatedLabelInInConjunctionLabelScan() throws Exception
    {
        // Given
        Node node = createNode();

        try ( org.neo4j.internal.kernel.api.Transaction tx = session.beginTransaction();
              NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor() )
        {
            // when
            int label1 = tx.tokenWrite().labelGetOrCreateForName( "labe1" );
            int label2 = tx.tokenWrite().labelGetOrCreateForName( "label2" );
            tx.dataWrite().nodeAddLabel( node.node, label2 );
            tx.dataRead().nodeLabelIntersectionScan( cursor, label1, label2 );

            // then
            assertFalse( cursor.next() );
        }
    }

    private void assertLabels( LabelSet labels, int... expected )
    {
        assertEquals( expected.length, labels.numberOfLabels() );
        Arrays.sort( expected );
        int[] labelArray = new int[labels.numberOfLabels()];
        for ( int i = 0; i < labels.numberOfLabels(); i++ )
        {
            labelArray[i] = labels.label( i );
        }
        Arrays.sort( labelArray );
        assertTrue( Arrays.equals( expected, labelArray ), "labels match expected" );
    }

    public Node createNode( String... labels ) throws Exception
    {
        long node;
        int[] labelIds = new int[labels.length];
        try ( Transaction tx = session.beginTransaction() )
        {
            Write write = tx.dataWrite();
            node = write.nodeCreate();

            for ( int i = 0; i < labels.length; i++ )
            {
                labelIds[i] = tx.tokenWrite().labelGetOrCreateForName( labels[i] );
                write.nodeAddLabel( node, labelIds[i] );
            }
            tx.success();
        }
        return new Node( node, labelIds );
    }

    private static class Node
    {
        private final long node;
        private final int[] labels;

        private Node( long node, int[] labels )
        {
            this.node = node;
            this.labels = labels;
        }

        public long node()
        {
            return node;
        }

        public int[] labels()
        {
            return labels;
        }
    }
}
