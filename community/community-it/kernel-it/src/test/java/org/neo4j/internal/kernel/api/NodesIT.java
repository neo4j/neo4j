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
package org.neo4j.internal.kernel.api;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
public class NodesIT
{
    private static final RelationshipType relationshipType = RelationshipType.withName( "relType" );

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void tracePageCacheAccessOnOutgoingSparse()
    {
        long nodeId = getSparseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 2, Nodes.countOutgoing( nodeCursor, cursors, cursorTracer ) );

                assertCursorEvents( cursorTracer, 1 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingSparseWithType()
    {
        long nodeId = getSparseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 2, Nodes.countOutgoing( nodeCursor, cursors, typeId, cursorTracer ) );

                assertCursorEvents( cursorTracer, 1 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDense()
    {
        long nodeId = getDenseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 100, Nodes.countOutgoing( nodeCursor, cursors, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseWithType()
    {
        long nodeId = getDenseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 100, Nodes.countOutgoing( nodeCursor, cursors, typeId, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingSparse()
    {
        long nodeId = getSparseIncomingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 2, Nodes.countIncoming( nodeCursor, cursors, cursorTracer ) );

                assertCursorEvents( cursorTracer, 1 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingSparseWithType()
    {
        long nodeId = getSparseIncomingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 2, Nodes.countIncoming( nodeCursor, cursors, typeId, cursorTracer ) );

                assertCursorEvents( cursorTracer, 1 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDense()
    {
        long nodeId = getDenseIncomingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 100, Nodes.countIncoming( nodeCursor, cursors, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseWithType()
    {
        long nodeId = getDenseIncomingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );

                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                assertEquals( 100, Nodes.countIncoming( nodeCursor, cursors, typeId, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllSparse()
    {
        long sparseIncomingId = getSparseIncomingNodeId();
        long sparseOutgoingId = getSparseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                setNodeCursor( sparseIncomingId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );
                assertEquals( 2, Nodes.countAll( nodeCursor, cursors, cursorTracer ) );

                setNodeCursor( sparseOutgoingId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );
                assertEquals( 2, Nodes.countAll( nodeCursor, cursors, cursorTracer ) );

                assertCursorEvents( cursorTracer, 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllSparseWithType()
    {
        long sparseIncomingId = getSparseIncomingNodeId();
        long sparseOutgoingId = getSparseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                setNodeCursor( sparseIncomingId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );
                assertEquals( 2, Nodes.countAll( nodeCursor, cursors, typeId, cursorTracer ) );
                setNodeCursor( sparseOutgoingId, kernelTransaction, nodeCursor );
                assertFalse( nodeCursor.isDense() );
                assertEquals( 2, Nodes.countAll( nodeCursor, cursors, typeId, cursorTracer ) );

                assertCursorEvents( cursorTracer, 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDense()
    {
        long denseIncomingId = getDenseIncomingNodeId();
        long denseOutgoingId = getDenseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                setNodeCursor( denseIncomingId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );
                assertEquals( 100, Nodes.countAll( nodeCursor, cursors, cursorTracer ) );

                setNodeCursor( denseOutgoingId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );
                assertEquals( 100, Nodes.countAll( nodeCursor, cursors, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseWithType()
    {
        long denseIncomingId = getDenseIncomingNodeId();
        long denseOutgoingId = getDenseOutgoingNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            var cursors = kernelTransaction.cursors();
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                setNodeCursor( denseIncomingId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );
                assertEquals( 100, Nodes.countAll( nodeCursor, cursors, typeId, cursorTracer ) );
                setNodeCursor( denseOutgoingId, kernelTransaction, nodeCursor );
                assertTrue( nodeCursor.isDense() );
                assertEquals( 100, Nodes.countAll( nodeCursor, cursors, typeId, cursorTracer ) );

                assertThat( cursorTracer.hits() ).isEqualTo( 2 );
                assertThat( cursorTracer.pins() ).isEqualTo( 2 );
                assertThat( cursorTracer.unpins() ).isEqualTo( 0 );
            }
        }
    }

    private void setNodeCursor( long nodeId, KernelTransaction kernelTransaction, NodeCursor nodeCursor )
    {
        kernelTransaction.dataRead().singleNode( nodeId, nodeCursor );
        assertTrue( nodeCursor.next() );
    }

    private void assertCursorEvents( PageCursorTracer cursorTracer, long events )
    {
        assertThat( cursorTracer.hits() ).isEqualTo( events );
        assertThat( cursorTracer.pins() ).isEqualTo( events );
        assertThat( cursorTracer.unpins() ).isEqualTo( events );
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }

    private long getDenseOutgoingNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var startNode = tx.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                var node = tx.createNode();
                startNode.createRelationshipTo( node, relationshipType );
            }
            long startNodeId = startNode.getId();
            tx.commit();
            return startNodeId;
        }
    }

    private long getDenseIncomingNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var startNode = tx.createNode();
            for ( int i = 0; i < 100; i++ )
            {
                var node = tx.createNode();
                node.createRelationshipTo( startNode, relationshipType );
            }
            long startNodeId = startNode.getId();
            tx.commit();
            return startNodeId;
        }
    }

    private long getSparseOutgoingNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var startNode = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            startNode.createRelationshipTo( endNode1, relationshipType );
            startNode.createRelationshipTo( endNode2, relationshipType );
            long startNodeId = startNode.getId();
            tx.commit();
            return startNodeId;
        }
    }

    private long getSparseIncomingNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var startNode = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            endNode1.createRelationshipTo( startNode, relationshipType );
            endNode2.createRelationshipTo( startNode, relationshipType );
            long startNodeId = startNode.getId();
            tx.commit();
            return startNodeId;
        }
    }

}
