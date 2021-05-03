/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEvent;

import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationshipCount;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationships;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEventKind.Relationship;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEventKind.RelationshipTypeScan;

abstract class RelationshipTypeIndexCursorTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private final int typeOne = 1;
    private final int typeTwo = 2;
    private final int typeThree = 3;

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class )
    void shouldFindRelationshipsByType( IndexOrder order ) throws KernelException
    {
        // GIVEN
        long toDelete;
        long relTwo;
        long relThree;
        long relTwo2;
        long relThree2;
        long relThree3;
        try ( KernelTransaction tx = beginTransaction() )
        {
            createRelationship( tx.dataWrite(), typeOne );
            relTwo = createRelationship( tx.dataWrite(), typeTwo );
            relThree = createRelationship( tx.dataWrite(), typeThree );
            toDelete = createRelationship( tx.dataWrite(), typeOne );
            relTwo2 = createRelationship( tx.dataWrite(), typeTwo );
            relThree2 = createRelationship( tx.dataWrite(), typeThree );
            relThree3 = createRelationship( tx.dataWrite(), typeThree );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( toDelete );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor( NULL ) )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // WHEN
                relationshipTypeScan( tx, typeOne, cursor, order );

                // THEN
                assertRelationshipCount( cursor, 1, uniqueIds );

                // WHEN
                relationshipTypeScan( tx, typeTwo, cursor, order );

                // THEN
                assertRelationships( cursor, uniqueIds, order, relTwo, relTwo2 );

                // WHEN
                relationshipTypeScan( tx, typeThree, cursor, order );

                // THEN
                assertRelationships( cursor, uniqueIds, order, relThree, relThree2, relThree3 );
            }
        }
    }

    @ParameterizedTest
    @EnumSource( value = IndexOrder.class )
    void shouldFindRelationshipsByTypeInTx( IndexOrder order ) throws KernelException
    {
        long inStore;
        long inStore2;
        long deletedInTx;
        long createdInTx;
        long createdInTx2;

        try ( KernelTransaction tx = beginTransaction() )
        {
            inStore = createRelationship( tx.dataWrite(), typeOne );
            createRelationship( tx.dataWrite(), typeTwo );
            deletedInTx = createRelationship( tx.dataWrite(), typeOne );
            inStore2 = createRelationship( tx.dataWrite(), typeOne );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( deletedInTx );
            createdInTx = createRelationship( tx.dataWrite(), typeOne );

            createRelationship( tx.dataWrite(), typeTwo );

            createdInTx2 = createRelationship( tx.dataWrite(), typeOne );

            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor( NULL ) )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // when
                relationshipTypeScan( tx, typeOne, cursor, order );

                // then
                assertRelationships( cursor, uniqueIds, order, inStore, inStore2, createdInTx, createdInTx2 );
            }
        }
    }

    @Test
    void shouldTraceRelationshipTypeScanEvents() throws KernelException
    {
        long first;
        long second;
        long third;
        try ( KernelTransaction tx = beginTransaction() )
        {
            first = createRelationship( tx.dataWrite(), typeOne );
            second = createRelationship( tx.dataWrite(), typeTwo );
            third = createRelationship( tx.dataWrite(), typeTwo );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            org.neo4j.internal.kernel.api.Read read = tx.dataRead();

            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor( NULL ) )
            {
                TestKernelReadTracer tracer = new TestKernelReadTracer();
                cursor.setTracer( tracer );

                // when
                relationshipTypeScan( tx, typeOne, cursor, IndexOrder.NONE );
                exhaustCursor( cursor );

                // then
                tracer.assertEvents(
                        new TraceEvent( RelationshipTypeScan, typeOne ),
                        new TraceEvent( Relationship, first ) );

                // when
                relationshipTypeScan( tx, typeTwo, cursor, IndexOrder.NONE );
                exhaustCursor( cursor );

                // then
                tracer.assertEvents(
                        new TraceEvent( RelationshipTypeScan, typeTwo ),
                        new TraceEvent( Relationship, second ),
                        new TraceEvent( Relationship, third ) );
            }
        }
    }

    private static void exhaustCursor( RelationshipTypeIndexCursor cursor )
    {
        while ( cursor.next() )
        {
        }
    }

    private static long createRelationship( Write write, int type ) throws KernelException
    {
        long sourceNode = write.nodeCreate();
        long targetNode = write.nodeCreate();
        return write.relationshipCreate( sourceNode, type, targetNode );
    }

    private void relationshipTypeScan( KernelTransaction tx, int label, RelationshipTypeIndexCursor cursor, IndexOrder indexOrder ) throws KernelException
    {
        IndexDescriptor index = tx.schemaRead().indexGetForName( "rti" );
        TokenReadSession tokenReadSession = tx.dataRead().tokenReadSession( index );
        tx.dataRead().relationshipTypeScan( tokenReadSession, cursor, IndexQueryConstraints.ordered( indexOrder ), new TokenPredicate( label ) );
    }
}
