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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodeCount;
import static org.neo4j.internal.kernel.api.IndexReadAsserts.assertNodes;

public abstract class NodeLabelIndexCursorTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G>
{
    private int labelOne = 1;
    private int labelTwo = 2;
    private int labelThree = 3;
    private int labelFirst = 4;

    @Test
    public void shouldFindNodesByLabel() throws Exception
    {
        // GIVEN
        long toDelete;
        try ( Transaction tx = session.beginTransaction() )
        {
            createNode( tx.dataWrite(), labelOne, labelFirst );
            createNode( tx.dataWrite(), labelTwo, labelFirst );
            createNode( tx.dataWrite(), labelThree, labelFirst );
            toDelete = createNode( tx.dataWrite(), labelOne );
            createNode( tx.dataWrite(), labelTwo );
            createNode( tx.dataWrite(), labelThree );
            createNode( tx.dataWrite(), labelThree );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().nodeDelete( toDelete );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            Read read = tx.dataRead();

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor();
                  PrimitiveLongSet uniqueIds = Primitive.longSet() )
            {
                // WHEN
                read.nodeLabelScan( labelOne, cursor );

                // THEN
                assertNodeCount( cursor, 1, uniqueIds );

                // WHEN
                read.nodeLabelScan( labelTwo, cursor );

                // THEN
                assertNodeCount( cursor, 2, uniqueIds );

                // WHEN
                read.nodeLabelScan( labelThree, cursor );

                // THEN
                assertNodeCount( cursor, 3, uniqueIds );

                // WHEN
                uniqueIds.clear();
                read.nodeLabelScan( labelFirst, cursor );

                // THEN
                assertNodeCount( cursor, 3, uniqueIds );
            }
        }
    }

    @Test
    public void shouldFindNodesByLabelInTx() throws Exception
    {
        long inStore;
        long deletedInTx;
        long createdInTx;

        try ( Transaction tx = session.beginTransaction() )
        {
            inStore = createNode( tx.dataWrite(), labelOne );
            createNode( tx.dataWrite(), labelTwo );
            deletedInTx = createNode( tx.dataWrite(), labelOne );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().nodeDelete( deletedInTx );
            createdInTx = createNode( tx.dataWrite(), labelOne );

            createNode( tx.dataWrite(), labelTwo );

            Read read = tx.dataRead();

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor();
                  PrimitiveLongSet uniqueIds = Primitive.longSet() )
            {
                // when
                read.nodeLabelScan( labelOne, cursor );

                // then
                assertNodes( cursor, uniqueIds, inStore, createdInTx );
            }
        }
    }

    private long createNode( Write write, int... labels ) throws KernelException
    {
        long nodeId = write.nodeCreate();
        for ( int label : labels )
        {
            write.nodeAddLabel( nodeId, label );
        }
        return nodeId;
    }
}
