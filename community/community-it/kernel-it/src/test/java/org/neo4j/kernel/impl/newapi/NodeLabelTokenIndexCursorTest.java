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

import java.util.concurrent.TimeUnit;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertNodeCount;
import static org.neo4j.kernel.impl.newapi.IndexReadAsserts.assertNodes;

public class NodeLabelTokenIndexCursorTest extends KernelAPIWriteTestBase<WriteTestSupport>
{
    @Override
    public WriteTestSupport newTestSupport()
    {
        return new WriteTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
            {
                builder = builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
                return super.configure( builder );
            }
        };
    }

    private int labelOne = 1;
    private int labelTwo = 2;
    private int labelThree = 3;
    private int labelFirst = 4;

    @Test
    void shouldFindNodesByLabel() throws Exception
    {
        // GIVEN
        createNLS();

        long toDelete;
        try ( KernelTransaction tx = beginTransaction() )
        {
            createNode( tx.dataWrite(), labelOne, labelFirst );
            createNode( tx.dataWrite(), labelTwo, labelFirst );
            createNode( tx.dataWrite(), labelThree, labelFirst );
            toDelete = createNode( tx.dataWrite(), labelOne );
            createNode( tx.dataWrite(), labelTwo );
            createNode( tx.dataWrite(), labelThree );
            createNode( tx.dataWrite(), labelThree );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().nodeDelete( toDelete );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            org.neo4j.internal.kernel.api.Read read = tx.dataRead();

            var session = getTokenReadSession( tx );

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( tx.pageCursorTracer() ) )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // WHEN
                read.nodeLabelScan( session, cursor, IndexQueryConstraints.unconstrained(), new TokenPredicate( labelOne ) );

                // THEN
                assertNodeCount( cursor, 1, uniqueIds );

                // WHEN
                read.nodeLabelScan( session, cursor, IndexQueryConstraints.unconstrained(), new TokenPredicate( labelTwo ) );

                // THEN
                assertNodeCount( cursor, 2, uniqueIds );

                // WHEN
                read.nodeLabelScan( session, cursor, IndexQueryConstraints.unconstrained(), new TokenPredicate( labelThree ) );

                // THEN
                assertNodeCount( cursor, 3, uniqueIds );

                // WHEN
                uniqueIds.clear();
                read.nodeLabelScan( session, cursor, IndexQueryConstraints.unconstrained(), new TokenPredicate( labelFirst ) );

                // THEN
                assertNodeCount( cursor, 3, uniqueIds );
            }
        }
    }

    private TokenReadSession getTokenReadSession( KernelTransaction tx ) throws IndexNotFoundKernelException
    {
        var descriptor = SchemaDescriptor.forAnyEntityTokens( EntityType.NODE );
        var indexes = tx.schemaRead().index( descriptor );
        var session = tx.dataRead().tokenReadSession( indexes.next() );
        return session;
    }

    @Test
    void shouldFindNodesByLabelInTx() throws Exception
    {

        createNLS();

        long inStore;
        long deletedInTx;
        long createdInTx;

        try ( KernelTransaction tx = beginTransaction() )
        {
            inStore = createNode( tx.dataWrite(), labelOne );
            createNode( tx.dataWrite(), labelTwo );
            deletedInTx = createNode( tx.dataWrite(), labelOne );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            tx.dataWrite().nodeDelete( deletedInTx );
            createdInTx = createNode( tx.dataWrite(), labelOne );

            createNode( tx.dataWrite(), labelTwo );

            Read read = tx.dataRead();

            var session = getTokenReadSession( tx );

            try ( NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor( tx.pageCursorTracer() ) )
            {
                MutableLongSet uniqueIds = new LongHashSet();

                // when
                read.nodeLabelScan( session, cursor, IndexQueryConstraints.unconstrained(), new TokenPredicate( labelOne ) );

                // then
                assertNodes( cursor, uniqueIds, inStore, createdInTx );
            }
        }
    }

    private void createNLS()
    {
        try ( var tx = graphDb.beginTx() )
        {
            tx.schema().indexFor( AnyTokens.ANY_LABELS ).create();
            tx.commit();
        }

        try ( var tx = graphDb.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
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
