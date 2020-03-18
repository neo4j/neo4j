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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
public class RelationshipSelectionsIT
{
    private static final RelationshipType relationshipType = withName( "relType" );

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void tracePageCacheAccessOnOutgoingCursor()
    {
        long nodeId = getSparseNodeId();
        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = outgoingCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingCursor()
    {
        long nodeId = getSparseNodeId();
        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = incomingCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllCursor()
    {
        var nodeId = getSparseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = allCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingIterator()
    {
        var nodeId = getSparseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = outgoingIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 2, count( iterator ) );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingIterator()
    {
        var nodeId = getSparseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = incomingIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 2, count( iterator ) );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllIterator()
    {
        var nodeId = getSparseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = allIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 4, count( iterator ) );
                }

                assertOneCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseCursor()
    {
        long nodeId = getDenseNodeId();
        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = outgoingCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseCursor()
    {
        long nodeId = getDenseNodeId();
        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = incomingCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseCursor()
    {
        var nodeId = getDenseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var cursor = allCursor( cursors, nodeCursor, new int[]{typeId}, cursorTracer ) )
                {
                    consumeCursor( cursor );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseIterator()
    {
        var nodeId = getDenseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = outgoingIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 2, count( iterator ) );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseIterator()
    {
        var nodeId = getDenseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = incomingIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 2, count( iterator ) );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseIterator()
    {
        var nodeId = getDenseNodeId();

        try ( var transaction = database.beginTx() )
        {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType( relationshipType.name() );
            try ( var nodeCursor = cursors.allocateNodeCursor( NULL ) )
            {
                setNodeCursor( nodeId, kernelTransaction, nodeCursor );
                var cursorTracer = kernelTransaction.pageCursorTracer();
                assertZeroCursor( cursorTracer );

                try ( var iterator = allIterator( cursors, nodeCursor, new int[]{typeId}, ( id, startNodeId, typeId1, endNodeId ) -> id, cursorTracer ) )
                {
                    assertEquals( 4, count( iterator ) );
                }

                assertTwoCursor( cursorTracer );
            }
        }
    }

    private long getSparseNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo( endNode1, relationshipType );
            source.createRelationshipTo( endNode2, relationshipType );
            endNode1.createRelationshipTo( source, relationshipType );
            endNode2.createRelationshipTo( source, relationshipType );
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private long getDenseNodeId()
    {
        try ( Transaction tx = database.beginTx() )
        {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo( endNode1, relationshipType );
            source.createRelationshipTo( endNode2, relationshipType );
            endNode1.createRelationshipTo( source, relationshipType );
            endNode2.createRelationshipTo( source, relationshipType );

            var other = withName( "other" );
            for ( int i = 0; i < 100; i++ )
            {
                var node = tx.createNode();
                source.createRelationshipTo( node, other );
            }
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private void setNodeCursor( long nodeId, KernelTransaction kernelTransaction, NodeCursor nodeCursor )
    {
        kernelTransaction.dataRead().singleNode( nodeId, nodeCursor );
        assertTrue( nodeCursor.next() );
    }

    private void consumeCursor( RelationshipTraversalCursor cursor )
    {
        while ( cursor.next() )
        {
            // consume cursor
        }
    }

    private void assertTwoCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isEqualTo( 2 );
        assertThat( cursorTracer.pins() ).isEqualTo( 2 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 2 );
    }

    private void assertOneCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isOne();
        assertThat( cursorTracer.pins() ).isOne();
        assertThat( cursorTracer.unpins() ).isOne();
    }

    private void assertZeroCursor( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }
}
