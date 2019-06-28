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
import org.mockito.internal.util.reflection.FieldSetter;

import java.util.stream.LongStream;

import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

class DefaultRelationshipTraversalCursorTest
{
    private static final long node = 42;
    private static final int type = 9999;
    private static final int type2 = 9998;
    private static final long relationship = 100;
    private static final long relationshipGroup = 313;
    private final DefaultPooledCursors pool = mock( DefaultPooledCursors.class );

    // Regular traversal of a sparse chain

    @Test
    void regularSparseTraversal() throws NoSuchFieldException
    {
        regularTraversal( relationship, false );
    }

    @Test
    void regularSparseTraversalWithTxState() throws NoSuchFieldException
    {
        regularTraversalWithTxState( relationship, false );
    }

    // Dense traversal is just like regular for this class, denseness is handled by the store

    @Test
    void regularDenseTraversal() throws NoSuchFieldException
    {
        regularTraversal( relationshipGroup, true );
    }

    @Test
    void regularDenseTraversalWithTxState() throws NoSuchFieldException
    {
        regularTraversalWithTxState( relationshipGroup, true );
    }

    // Sparse traversal but with tx-state filtering

    @Test
    void sparseTraversalWithTxStateFiltering() throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor =
                storeCursor(
                        rel( 100, node, 50, type ), // <- the filter template
                        rel( 102, node, 51, type ),
                        rel( 104, node, 52, type ) );

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, relationship,
                // relationships of a specific type/direction
                (int) NO_ID, null, true, read );

        // then
        assertRelationships( cursor, 100, 3, 7, 102, 104 );
    }

    // Sparse traversal but with filtering both of store and tx-state

    @Test
    void sparseTraversalWithFiltering() throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor =
                storeCursor(
                        rel( 100, 50, node, type ), // <- the filter template
                        rel( 103, 51, node, type ) );

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, relationship,
                // relationships of a specific type/direction
                (int) NO_ID, null, false, read );

        // then
        assertRelationships( cursor, 100, 4, 103 );
    }

    // Empty store, but filter tx-state

    @Test
    void emptyStoreOutgoingOfType() throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, node, 50, type2 ),
                rel( 6, node, node, type ),
                rel( 7, node, 52, type )
        );

        // when
        cursor.init( node, relationship, type, OUTGOING, false, read );

        // then
        assertRelationships( cursor, 3, 7 );
    }

    @Test
    void emptyStoreIncomingOfType() throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 4, 50, node, type ),
                rel( 5, 50, node, type2 ),
                rel( 6, node, node, type ),
                rel( 7, 56, node, type ),
                rel( 8, node, 52, type )
        );

        // when
        cursor.init( node, relationship, type, INCOMING, false, read );

        // then
        assertRelationships( cursor, 4, 7 );
    }

    @Test
    void emptyStoreLoopsOfType() throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = emptyStoreCursor();

        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState(
                rel( 3, node, 50, type ),
                rel( 2, node, node, type ),
                rel( 5, 50, node, type2 ),
                rel( 6, node, node, type ),
                rel( 7, 56, node, type ),
                rel( 8, node, 52, type )
        );

        // when
        cursor.init( node, relationship, type, LOOP, false, read );

        // then
        assertRelationships( cursor, 2, 6 );
    }

    // HELPERS

    private void regularTraversal( long reference, boolean dense ) throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor( 100, 102, 104 );
        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = emptyTxState();

        // when
        cursor.init( node, reference, dense, read );

        // then
        assertRelationships( cursor, 100, 102, 104 );
    }

    private void regularTraversalWithTxState( long reference, boolean dense ) throws NoSuchFieldException
    {
        // given
        StorageRelationshipTraversalCursor storeCursor = storeCursor( 100, 102, 104 );
        DefaultRelationshipTraversalCursor cursor = new DefaultRelationshipTraversalCursor( pool::accept, storeCursor );
        Read read = txState( 3, 4 );

        // when
        cursor.init( node, reference, dense, read );

        // then
        assertRelationships( cursor, 3, 4, 100, 102, 104 );
    }

    private static Read emptyTxState() throws NoSuchFieldException
    {
        Read read = mock( Read.class );
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        FieldSetter.setField(read, Read.class.getDeclaredField("ktx"), ktx);
        when( ktx.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );
        return read;
    }

    private static Read txState( long... ids ) throws NoSuchFieldException
    {
        return txState( LongStream.of( ids ).mapToObj( id -> rel( id, node, node, type ) ).toArray( Rel[]::new ) );
    }

    private static Read txState( Rel... rels ) throws NoSuchFieldException
    {
        Read read = mock( Read.class );
        KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
        FieldSetter.setField(read, Read.class.getDeclaredField("ktx"), ktx);
        when( ktx.securityContext() ).thenReturn( SecurityContext.AUTH_DISABLED );
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

    private static void assertRelationships( DefaultRelationshipTraversalCursor cursor, long... expected )
    {
        for ( long expectedId : expected )
        {
            assertTrue( cursor.next(), "Expected relationship " + expectedId + " but got none" );
            assertEquals( expectedId, cursor.relationshipReference(),
                          "Expected relationship " + expectedId + " got " + cursor.relationshipReference() );
        }
        assertFalse( cursor.next(), "Expected no more relationships, but got " + cursor.relationshipReference() );
    }

    private static Rel rel( long relId, long startId, long endId, int type )
    {
        return new Rel( relId, startId, endId, type );
    }

    private static final Rel NO_REL = rel( -1L, -1L, -1L, -1 );

    private static class Rel
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

    private static StorageRelationshipTraversalCursor emptyStoreCursor()
    {
        return storeCursor( new Rel[0] );
    }

    private static StorageRelationshipTraversalCursor storeCursor( long... ids )
    {
        return storeCursor( LongStream.of( ids ).mapToObj( id -> rel( id, -1L, -1L, -1 ) ).toArray( Rel[]::new ) );
    }

    private static StorageRelationshipTraversalCursor storeCursor( Rel... rels )
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
            public void init( long nodeReference, long reference, boolean nodeIsDense )
            {
            }

            @Override
            public void init( long nodeReference, long reference, int type, RelationshipDirection direction, boolean nodeIsDense )
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
            public void properties( StoragePropertyCursor propertyCursor )
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
