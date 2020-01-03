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

import org.junit.Test;

import java.util.stream.LongStream;

import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultRelationshipTraversalCursorTest
{
    private DefaultCursors pool = mock( DefaultCursors.class );
    private long node = 42;
    private int type = 9999;
    private int type2 = 9998;
    private long relationship = 100;
    private long relationshipGroup = 313;

    // Regular traversal of a sparse chain

    @Test
    public void regularSparseTraversal()
    {
        regularTraversal( relationship );
    }

    @Test
    public void regularSparseTraversalWithTxState()
    {
        regularTraversalWithTxState( relationship );
    }

    // Dense traversal is just like regular for this class, denseness is handled by the store

    @Test
    public void regularDenseTraversal()
    {
        regularTraversal( RelationshipReferenceEncoding.encodeGroup( relationshipGroup ) );
    }

    @Test
    public void regularDenseTraversalWithTxState()
    {
        regularTraversalWithTxState( RelationshipReferenceEncoding.encodeGroup( relationshipGroup ) );
    }

    // Sparse traversal but with tx-state filtering

    @Test
    public void sparseTraversalWithTxStateFiltering()
    {
        // given
        StorageRelationshipTraversalCursor storeCursor =
                storeCursor(
                        rel( 100, node, 50, type ), // <- the filter template
                        rel( 102, node, 51, type ),
                        rel( 104, node, 52, type ) );

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, RelationshipReferenceEncoding.encodeForTxStateFiltering( relationship ), read );

        // then
        assertRelationships( cursor, 100, 3, 7, 102, 104 );
    }

    // Sparse traversal but with filtering both of store and tx-state

    @Test
    public void sparseTraversalWithFiltering()
    {
        // given
        StorageRelationshipTraversalCursor storeCursor =
                storeCursor(
                        rel( 100, 50, node, type ), // <- the filter template
                        rel( 101, node, 50, type ),
                        rel( 102, 50, node, type2 ),
                        rel( 103, 51, node, type ),
                        rel( 104, node, node, type ) );

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, RelationshipReferenceEncoding.encodeForFiltering( relationship ), read );

        // then
        assertRelationships( cursor, 100, 4, 103 );
    }

    // Empty store, but filter tx-state

    @Test
    public void emptyStoreOutgoingOfType()
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, RelationshipReferenceEncoding.encodeNoOutgoingRels( type ), read );

        // then
        assertRelationships( cursor, 3, 7 );
    }

    @Test
    public void emptyStoreIncomingOfType()
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, 50, node, type2 ),
                rel( 6, node, node, type ),
                rel( 7, 56, node, type ),
                rel( 8, node, 52, type )
        );

        // when
        cursor.init( node, RelationshipReferenceEncoding.encodeNoIncomingRels( type ), read );

        // then
        assertRelationships( cursor, 4, 7 );
    }

    @Test
    public void emptyStoreLoopsOfType()
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 2, node, node, type ),
                rel( 5, 50, node, type2 ),
                rel( 6, node, node, type ),
                rel( 7, 56, node, type ),
                rel( 8, node, 52, type )
        );

        // when
        cursor.init( node, RelationshipReferenceEncoding.encodeNoLoopRels( type ), read );

        // then
        assertRelationships( cursor, 2, 6 );
    }

    // HELPERS

    private void regularTraversal( long reference )
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor( 100, 102, 104 );
        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = emptyTxState();

        // when
        cursor.init( node, reference, read );

        // then
        assertRelationships( cursor, 100, 102, 104 );
    }

    private void regularTraversalWithTxState( long reference )
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor( 100, 102, 104 );
        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool, storeCursor );
        Read read = txState( 3, 4 );

        // when
        cursor.init( node, reference, read );

        // then
        assertRelationships( cursor, 3, 4, 100, 102, 104 );
    }

    private Read emptyTxState()
    {
        return mock( Read.class );
    }

    private Read txState( long... ids )
    {
        return txState( LongStream.of( ids ).mapToObj( id -> rel( id, node, node, type ) ).toArray( Rel[]::new ) );
    }

    private Read txState( Rel... rels )
    {
        Read read = mock( Read.class );
        if ( rels.length > 0 )
        {
            TxState txState = new TxState();
            for ( Rel rel : rels )
            {
                txState.relationshipDoCreate( rel.relId, rel.type, rel.sourceId, rel.targetId );
            }
            when( read.hasTxStateWithChanges() ).thenReturn( true );
            when( read.txState() ).thenReturn( txState );
        }
        return read;
    }

    private void assertRelationships( DefaultRelationshipTraversalCursor cursor, long... expected )
    {
        for ( long expectedId : expected )
        {
            assertTrue( cursor.next(), "Expected relationship " + expectedId + " but got none" );
            assertEquals( expectedId, cursor.relationshipReference(),
                          "Expected relationship " + expectedId + " got " + cursor.relationshipReference() );
        }
        assertFalse( cursor.next(), "Expected no more relationships, but got " + cursor.relationshipReference() );
    }

    private Rel rel( long relId, long startId, long endId, int type )
    {
        return new Rel( relId, startId, endId, type );
    }

    private Rel NO_REL = rel( -1L, -1L, -1L, -1 );

    private class Rel
    {
        final long relId;
        final long sourceId;
        final long targetId;
        final int type;

        Rel( long relId, long sourceId, long targetId, int type )
        {
            this.relId = relId;
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.type = type;
        }
    }

    private StorageRelationshipTraversalCursor emptyStoreCursor( long... ids )
    {
        return storeCursor( new Rel[0] );
    }

    private StorageRelationshipTraversalCursor storeCursor( long... ids )
    {
        return storeCursor( LongStream.of( ids ).mapToObj( id -> rel( id, -1L, -1L, -1 ) ).toArray( Rel[]::new ) );
    }

    private StorageRelationshipTraversalCursor storeCursor( Rel... rels )
    {
        return new StorageRelationshipTraversalCursor()
        {
            private int i = -1;
            private Rel rel = NO_REL;

            @Override
            public long neighbourNodeReference()
            {
                return rel.sourceId == node ? rel.targetId : rel.sourceId;
            }

            @Override
            public long originNodeReference()
            {
                return node;
            }

            @Override
            public void init( long nodeReference, long reference )
            {
            }

            @Override
            public int type()
            {
                return rel.type;
            }

            @Override
            public long sourceNodeReference()
            {
                return rel.sourceId;
            }

            @Override
            public long targetNodeReference()
            {
                return rel.targetId;
            }

            @Override
            public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
            {
                rel = rel( relationshipId, startNodeId, endNodeId, typeId );
            }

            @Override
            public boolean hasProperties()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public long propertiesReference()
            {
                throw new UnsupportedOperationException( "not implemented" );
            }

            @Override
            public long entityReference()
            {
                return rel.relId;
            }

            @Override
            public boolean next()
            {
                i++;
                if ( i < 0 || i >= rels.length )
                {
                    rel = NO_REL;
                    return false;
                }
                else
                {
                    rel = rels[i];
                    return true;
                }
            }

            @Override
            public void reset()
            {
            }

            @Override
            public void close()
            {
            }
        };
    }
}
