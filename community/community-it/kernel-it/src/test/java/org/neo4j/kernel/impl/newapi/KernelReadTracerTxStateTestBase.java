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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnAllNodesScan;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnIndexSeek;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnLabelScan;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnNode;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnProperty;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnRelationship;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.OnRelationshipGroup;

abstract class KernelReadTracerTxStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    @Test
    void shouldTraceAllNodesScan() throws Exception
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              NodeCursor cursor = tx.cursors().allocateNodeCursor() )
        {
            tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeCreate();

            // when
            cursor.setTracer( tracer );
            tx.dataRead().allNodesScan( cursor );
            tracer.assertEvents( OnAllNodesScan );

            cursor.next();
            tracer.assertEvents( OnNode( cursor.nodeReference() ) );

            cursor.next();
            tracer.assertEvents( OnNode( cursor.nodeReference() ) );

            assertFalse( cursor.next() );
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceLabelScan() throws KernelException
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor() )
        {
            int barId = tx.tokenWrite().labelGetOrCreateForName( "Bar" );
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel( n, barId );

            // when
            cursor.setTracer( tracer );
            tx.dataRead().nodeLabelScan( barId, cursor );
            tracer.assertEvents( OnLabelScan( barId ) );

            cursor.next();
            tracer.assertEvents( OnNode( cursor.nodeReference() ) );

            assertFalse( cursor.next() );
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceIndexSeek() throws KernelException
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        createIndex( "User", "name" );

        try ( Transaction tx = beginTransaction();
              NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor() )
        {
            int name = tx.token().propertyKey( "name" );
            int user = tx.token().nodeLabel( "User" );
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel( n, user );
            tx.dataWrite().nodeSetProperty( n, name, Values.stringValue( "Bosse" ) );
            IndexReference index = tx.schemaRead().index( user, name );
            IndexReadSession session = tx.dataRead().indexReadSession( index );

            // when
            assertIndexSeekTracing( tracer, tx, cursor, session, IndexOrder.NONE, false, user );
            assertIndexSeekTracing( tracer, tx, cursor, session, IndexOrder.NONE, true, user );
            assertIndexSeekTracing( tracer, tx, cursor, session, IndexOrder.ASCENDING, false, user );
            assertIndexSeekTracing( tracer, tx, cursor, session, IndexOrder.ASCENDING, true, user );
        }
    }

    private void assertIndexSeekTracing( TestKernelReadTracer tracer,
                                         Transaction tx,
                                         NodeValueIndexCursor cursor,
                                         IndexReadSession session,
                                         IndexOrder order,
                                         boolean needsValues,
                                         int user ) throws KernelException
    {
        cursor.setTracer( tracer );

        tx.dataRead().nodeIndexSeek( session, cursor, order, needsValues, IndexQuery.stringPrefix( user, Values.stringValue( "B" ) ) );
        tracer.assertEvents( OnIndexSeek() );

        cursor.next();
        tracer.assertEvents( OnNode( cursor.nodeReference() ) );

        assertFalse( cursor.next() );
        tracer.assertEvents();
    }

    @Test
    void shouldTraceSingleRelationship() throws Exception
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor() )
        {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            long r = tx.dataWrite().relationshipCreate( n1, tx.token().relationshipTypeGetOrCreateForName( "R" ), n2 );

            // when
            cursor.setTracer( tracer );
            tx.dataRead().singleRelationship( r, cursor );

            cursor.next();
            tracer.assertEvents( OnRelationship( r ) );

            long deleted = tx.dataWrite().relationshipCreate( n1, tx.token().relationshipTypeGetOrCreateForName( "R" ), n2 );
            tx.dataWrite().relationshipDelete( deleted );

            tx.dataRead().singleRelationship( deleted, cursor );
            assertFalse( cursor.next() );
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceRelationshipTraversal() throws Exception
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              RelationshipTraversalCursor cursor = tx.cursors().allocateRelationshipTraversalCursor() )
        {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            long r = tx.dataWrite().relationshipCreate( n1, tx.token().relationshipTypeGetOrCreateForName( "R" ), n2 );

            // when
            cursor.setTracer( tracer );
            tx.dataRead().singleNode( n1, nodeCursor );
            nodeCursor.next();
            nodeCursor.allRelationships( cursor );

            cursor.next();
            tracer.assertEvents( OnRelationship( r ) );

            assertFalse( cursor.next() );
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceGroupTraversal() throws Exception
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              RelationshipGroupCursor cursor = tx.cursors().allocateRelationshipGroupCursor() )
        {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( n1, tx.token().relationshipTypeGetOrCreateForName( "R" ), n2 );

            // when
            cursor.setTracer( tracer );
            tx.dataRead().singleNode( n1, nodeCursor );
            nodeCursor.next();
            nodeCursor.relationships( cursor );

            cursor.next();
            int expectedType = cursor.type();
            tracer.assertEvents( OnRelationshipGroup( expectedType ) );

            assertFalse( cursor.next() );
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTracePropertyAccess() throws Exception
    {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try ( Transaction tx = beginTransaction();
              NodeCursor nodeCursor = tx.cursors().allocateNodeCursor();
              PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor() )
        {
            long n = tx.dataWrite().nodeCreate();
            int name = tx.token().propertyKey( "name" );
            tx.dataWrite().nodeSetProperty( n, name, Values.stringValue( "Bosse" ) );

            // when
            propertyCursor.setTracer( tracer );

            tx.dataRead().singleNode( n, nodeCursor );
            nodeCursor.next();
            nodeCursor.properties( propertyCursor );

            propertyCursor.next();
            tracer.assertEvents( OnProperty( name ) );

            assertFalse( propertyCursor.next() );
            tracer.assertEvents();
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private void createIndex( String label, String propertyKey )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( Label.label( label ) ).on( propertyKey ).create();
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().awaitIndexesOnline( 1, MINUTES );
        }
    }
}
