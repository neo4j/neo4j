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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEvent;

import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationshipCount;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertRelationships;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEventKind.Relationship;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEventKind.RelationshipTypeScan;

abstract class RelationshipTypeIndexCursorTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private int typeOne = 1;
    private int typeTwo = 2;
    private int typeThree = 3;

    @Test
    void shouldFindRelationshipsByType() throws KernelException
    {
        // GIVEN
        long toDelete;
        try ( KernelTransaction tx = beginTransaction() )
        {
            createRelationship( tx.dataWrite(), typeOne );
            createRelationship( tx.dataWrite(), typeTwo );
            createRelationship( tx.dataWrite(), typeThree );
            toDelete = createRelationship( tx.dataWrite(), typeOne );
            createRelationship( tx.dataWrite(), typeTwo );
            createRelationship( tx.dataWrite(), typeThree );
            createRelationship( tx.dataWrite(), typeThree );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( toDelete );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            org.neo4j.internal.kernel.api.Read read = tx.dataRead();

            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor() )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // WHEN
                read.relationshipTypeScan( typeOne, cursor );

                // THEN
                assertRelationshipCount( cursor, 1, uniqueIds );

                // WHEN
                read.relationshipTypeScan( typeTwo, cursor );

                // THEN
                assertRelationshipCount( cursor, 2, uniqueIds );

                // WHEN
                read.relationshipTypeScan( typeThree, cursor );

                // THEN
                assertRelationshipCount( cursor, 3, uniqueIds );
            }
        }
    }

    @Test
    void shouldFindRelationshipsByTypeInTx() throws KernelException
    {
        long inStore;
        long deletedInTx;
        long createdInTx;

        try ( KernelTransaction tx = beginTransaction() )
        {
            inStore = createRelationship( tx.dataWrite(), typeOne );
            createRelationship( tx.dataWrite(), typeTwo );
            deletedInTx = createRelationship( tx.dataWrite(), typeOne );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( deletedInTx );
            createdInTx = createRelationship( tx.dataWrite(), typeOne );

            createRelationship( tx.dataWrite(), typeTwo );

            Read read = tx.dataRead();

            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor() )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // when
                read.relationshipTypeScan( typeOne, cursor );

                // then
                assertRelationships( cursor, uniqueIds, inStore, createdInTx );
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

            try ( RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor() )
            {
                TestKernelReadTracer tracer = new TestKernelReadTracer();
                cursor.setTracer( tracer );
                MutableLongSet uniqueIds = new LongHashSet();

                // when
                read.relationshipTypeScan( typeOne, cursor );
                exhaustCursor( cursor );

                // then
                tracer.assertEvents(
                        new TraceEvent( RelationshipTypeScan, typeOne ),
                        new TraceEvent( Relationship, first ) );

                // when
                read.relationshipTypeScan( typeTwo, cursor );
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
}
