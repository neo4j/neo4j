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
package org.neo4j.kernel.impl.newapi;


import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.TestAccessMode;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.ValueGroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class NodeTransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    void shouldSeeNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );
                assertEquals( nodeId, node.nodeReference() );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertEquals( nodeId, tx.getNodeById( nodeId ).getId() );
        }
    }

    @Test
    void shouldSeeNewLabeledNodeInTransaction() throws Exception
    {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.token().labelGetOrCreateForName( labelName );
            tx.dataWrite().nodeAddLabel( nodeId, labelId );

            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                TokenSet labels = node.labels();
                assertEquals( 1, labels.numberOfTokens() );
                assertEquals( labelId, labels.token( 0 ) );
                assertTrue( node.hasLabel( labelId ) );
                assertFalse( node.hasLabel( labelId + 1 ) );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getLabels() ).isEqualTo( Iterables.iterable( label( labelName ) ) );
        }
    }

    @Test
    void shouldSeeLabelChangesInTransaction() throws Exception
    {
        long nodeId;
        int toRetain, toDelete, toAdd, toRegret;
        final String toRetainName = "ToRetain";
        final String toDeleteName = "ToDelete";
        final String toAddName = "ToAdd";
        final String toRegretName = "ToRegret";

        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            toRetain = tx.token().labelGetOrCreateForName( toRetainName );
            toDelete = tx.token().labelGetOrCreateForName( toDeleteName );
            tx.dataWrite().nodeAddLabel( nodeId, toRetain );
            tx.dataWrite().nodeAddLabel( nodeId, toDelete );
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getLabels() ).contains( label( toRetainName ), label( toDeleteName ) );
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            toAdd = tx.token().labelGetOrCreateForName( toAddName );
            tx.dataWrite().nodeAddLabel( nodeId, toAdd );
            tx.dataWrite().nodeRemoveLabel( nodeId, toDelete );

            toRegret = tx.token().labelGetOrCreateForName( toRegretName );
            tx.dataWrite().nodeAddLabel( nodeId, toRegret );
            tx.dataWrite().nodeRemoveLabel( nodeId, toRegret );

            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                assertLabels( node.labels(), toRetain, toAdd );
                assertTrue( node.hasLabel( toAdd ) );
                assertTrue( node.hasLabel( toRetain ) );
                assertFalse( node.hasLabel( toDelete ) );
                assertFalse( node.hasLabel( toRegret ) );
                assertFalse( node.next(), "should only find one node" );
            }
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getLabels() ).contains( label( toRetainName ), label( toAddName ) );
        }
    }

    @Test
    void shouldDiscoverDeletedNodeInTransaction() throws Exception
    {
        long nodeId;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertFalse( node.next() );
            }
            tx.commit();
        }
    }

    @Test
    void shouldHandleMultipleNodeDeletions() throws Exception
    {
        long nodeId;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            assertTrue( tx.dataWrite().nodeDelete( nodeId ) );
            assertFalse( tx.dataWrite().nodeDelete( nodeId ) );
            tx.commit();
        }
    }

    @Test
    void shouldSeeNewNodePropertyInTransaction() throws Exception
    {
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";

        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            int prop1 = tx.token().propertyKeyGetOrCreateForName( propKey1 );
            int prop2 = tx.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, prop1, stringValue( "hello" ) ), NO_VALUE );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, prop2, stringValue( "world" ) ), NO_VALUE );

            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
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
            tx.commit();
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingNodeWithoutPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // When/Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            int propToken = tx.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );

            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
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

            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( propKey ) ).isEqualTo( "hello" );
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";
        int propToken1;
        int propToken2;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken1 = tx.token().propertyKeyGetOrCreateForName( propKey1 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken1, stringValue( "hello" ) ), NO_VALUE );
            tx.commit();
        }

        // When/Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            propToken2 = tx.token().propertyKeyGetOrCreateForName( propKey2 );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken2, stringValue( "world" ) ), NO_VALUE );

            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
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
            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( propKey1 ) ).isEqualTo( "hello" );
            assertThat( tx.getNodeById( nodeId ).getProperty( propKey2 ) ).isEqualTo( "world" );
        }
    }

    @Test
    void shouldSeeUpdatedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.commit();
        }

        // When/Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "world" ) ),
                    stringValue( "hello" ) );
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
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

            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( propKey ) ).isEqualTo( "world" );
        }
    }

    @Test
    void shouldSeeRemovedPropertyInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.commit();
        }

        // When/Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeRemoveProperty( nodeId, propToken ), stringValue( "hello" ) );
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( nodeId, node );
                assertTrue( node.next(), "should access node" );

                node.properties( property );
                assertFalse( property.next(), "should not find any properties" );
                assertFalse( node.next(), "should only find one node" );
            }

            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertFalse(
                    tx.getNodeById( nodeId ).hasProperty( propKey ) );
        }
    }

    @Test
    void shouldSeeRemovedThenAddedPropertyInTransaction() throws Exception
    {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try ( KernelTransaction tx = beginTransaction() )
        {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName( propKey );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "hello" ) ), NO_VALUE );
            tx.commit();
        }

        // When/Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertEquals( tx.dataWrite().nodeRemoveProperty( nodeId, propToken ), stringValue( "hello" ) );
            assertEquals( tx.dataWrite().nodeSetProperty( nodeId, propToken, stringValue( "world" ) ), NO_VALUE );
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor property = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
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

            tx.commit();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            assertThat( tx.getNodeById( nodeId ).getProperty( propKey ) ).isEqualTo( "world" );
        }
    }

    @Test
    void shouldSeeExistingNode() throws Exception
    {
        // Given
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertTrue( tx.dataRead().nodeExists( node ) );
        }
    }

    @Test
    void shouldNotSeeNonExistingNode() throws Exception
    {
        // Given, empty db

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            assertFalse( tx.dataRead().nodeExists( 1337L ) );
        }
    }

    @Test
    void shouldSeeNodeExistingInTxOnly() throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            long node = tx.dataWrite().nodeCreate();
            assertTrue( tx.dataRead().nodeExists( node ) );

        }
    }

    @Test
    void shouldNotSeeDeletedNode() throws Exception
    {
        // Given
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().nodeDelete( node );
            assertFalse( tx.dataRead().nodeExists( node ) );
        }
    }

    @Test
    void shouldNotFindDeletedNodeInLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label" );

        try ( KernelTransaction tx = beginTransaction();
              NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeDelete( node.node );
            tx.dataRead().nodeLabelScan( node.labels[0], cursor );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldNotFindNodeWithRemovedLabelInLabelScan() throws Exception
    {
        // Given
        Node node = createNode( "label" );

        try ( KernelTransaction tx = beginTransaction();
              NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node.node, node.labels[0] );
            tx.dataRead().nodeLabelScan( node.labels[0], cursor );

            // then
            assertFalse( cursor.next() );
        }
    }

    @Test
    void shouldFindUpdatedNodeInInLabelScan() throws Exception
    {
        // Given
        Node node = createNode();

        try ( KernelTransaction tx = beginTransaction();
              NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
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
    void shouldFindSwappedNodeInLabelScan() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode();

        try ( KernelTransaction tx = beginTransaction();
              NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
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

    @Test
    void shouldCountNewLabelsFromTxState() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode();

        try ( KernelTransaction tx = beginTransaction() )
        {
            // when
            tx.dataWrite().nodeAddLabel( node2.node, node1.labels[0] );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 2, countTxState );
            assertEquals( 1, countNoTxState );
        }
    }

    @Test
    void shouldCountNewNodesFromTxState() throws Exception
    {
        // Given
        createNode();
        createNode();

        try ( KernelTransaction tx = beginTransaction() )
        {
            // when
            tx.dataWrite().nodeCreate();
            long countTxState = tx.dataRead().countsForNode( -1 );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( -1 );

            // then
            assertEquals( 3, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void shouldNotCountRemovedLabelsFromTxState() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode( "label" );

        try ( KernelTransaction tx = beginTransaction() )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node2.node, node2.labels[0] );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 1, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void shouldNotCountRemovedNodesFromTxState() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode( "label" );

        try ( KernelTransaction tx = beginTransaction() )
        {
            // when
            tx.dataWrite().nodeDelete( node2.node );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 1, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void shouldCountNewLabelsFromTxStateRestrictedUser() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode();

        SecurityContext loginContext = new SecurityContext( AuthSubject.AUTH_DISABLED, new TestAccessMode( true, false, true, false ) );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            // when
            tx.dataWrite().nodeAddLabel( node2.node, node1.labels[0] );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 2, countTxState );
            assertEquals( 1, countNoTxState );
        }
    }

    @Test
    void shouldCountNewNodesFromTxStateRestrictedUser() throws Exception
    {
        // Given
        createNode();
        createNode();

        SecurityContext loginContext = new SecurityContext( AuthSubject.AUTH_DISABLED, new TestAccessMode( true, false, true, false ) );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            // when
            tx.dataWrite().nodeCreate();
            long countTxState = tx.dataRead().countsForNode( -1 );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( -1 );

            // then
            assertEquals( 3, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void shouldNotCountRemovedLabelsFromTxStateRestrictedUser() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode( "label" );

        SecurityContext loginContext = new SecurityContext( AuthSubject.AUTH_DISABLED, new TestAccessMode( true, false, true, false ) );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            // when
            tx.dataWrite().nodeRemoveLabel( node2.node, node2.labels[0] );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 1, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void shouldNotCountRemovedNodesFromTxStateRestrictedUser() throws Exception
    {
        // Given
        Node node1 = createNode( "label" );
        Node node2 = createNode( "label" );

        SecurityContext loginContext = new SecurityContext( AuthSubject.AUTH_DISABLED, new TestAccessMode( true, false, true, false ) );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            // when
            tx.dataWrite().nodeDelete( node2.node );
            long countTxState = tx.dataRead().countsForNode( node1.labels[0] );
            long countNoTxState = tx.dataRead().countsForNodeWithoutTxState( node1.labels[0] );

            // then
            assertEquals( 1, countTxState );
            assertEquals( 2, countNoTxState );
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedProperties() throws Exception
    {
        // Given
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor cursor = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor props = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( node, cursor );
                assertTrue( cursor.next() );
                assertFalse( hasProperties( cursor, props ) );
                tx.dataWrite().nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ),
                        stringValue( "foo" ) );
                assertTrue( hasProperties( cursor, props ) );
            }
        }
    }

    private boolean hasProperties( NodeCursor cursor, PropertyCursor props )
    {
        cursor.properties( props );
        return props.next();
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedPropertiesOnNewlyCreatedNode() throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            long node = tx.dataWrite().nodeCreate();
            try ( NodeCursor cursor = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor props = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( node, cursor );
                assertTrue( cursor.next() );
                assertFalse( hasProperties( cursor, props ) );
                tx.dataWrite().nodeSetProperty( node, tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" ),
                        stringValue( "foo" ) );
                assertTrue( hasProperties( cursor, props ) );
            }
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyRemovedProperties() throws Exception
    {
        // Given
        long node;
        int prop1, prop2, prop3;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
            prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
            prop3 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
            tx.dataWrite().nodeSetProperty( node, prop1, longValue( 1 ) );
            tx.dataWrite().nodeSetProperty( node, prop2, longValue( 2 ) );
            tx.dataWrite().nodeSetProperty( node, prop3, longValue( 3 ) );
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor cursor = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor props = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( node, cursor );
                assertTrue( cursor.next() );

                assertTrue( hasProperties( cursor, props ) );
                tx.dataWrite().nodeRemoveProperty( node, prop1 );
                assertTrue( hasProperties( cursor, props ) );
                tx.dataWrite().nodeRemoveProperty( node, prop2 );
                assertTrue( hasProperties( cursor, props ) );
                tx.dataWrite().nodeRemoveProperty( node, prop3 );
                assertFalse( hasProperties( cursor, props ) );
            }
        }
    }

    @Test
    void propertyTypeShouldBeTxStateAware() throws Exception
    {
        // Given
        long node;
        try ( KernelTransaction tx = beginTransaction() )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodes = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() );
                  PropertyCursor properties = tx.cursors().allocatePropertyCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().singleNode( node, nodes );
                assertTrue( nodes.next() );
                assertFalse( hasProperties( nodes, properties ) );
                int prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
                tx.dataWrite().nodeSetProperty( node, prop, stringValue( "foo" ) );
                nodes.properties( properties );

                assertTrue( properties.next() );
                assertThat( properties.propertyType() ).isEqualTo( ValueGroup.TEXT );
            }
        }
    }

    private void assertLabels( TokenSet labels, int... expected )
    {
        assertEquals( expected.length, labels.numberOfTokens() );
        Arrays.sort( expected );
        int[] labelArray = new int[labels.numberOfTokens()];
        for ( int i = 0; i < labels.numberOfTokens(); i++ )
        {
            labelArray[i] = labels.token( i );
        }
        Arrays.sort( labelArray );
        assertTrue( Arrays.equals( expected, labelArray ), "labels match expected" );
    }

    public Node createNode( String... labels ) throws Exception
    {
        long node;
        int[] labelIds = new int[labels.length];
        try ( KernelTransaction tx = beginTransaction() )
        {
            Write write = tx.dataWrite();
            node = write.nodeCreate();

            for ( int i = 0; i < labels.length; i++ )
            {
                labelIds[i] = tx.tokenWrite().labelGetOrCreateForName( labels[i] );
                write.nodeAddLabel( node, labelIds[i] );
            }
            tx.commit();
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
