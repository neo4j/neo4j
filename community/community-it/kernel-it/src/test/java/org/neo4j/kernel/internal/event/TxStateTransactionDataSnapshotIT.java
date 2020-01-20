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
package org.neo4j.kernel.internal.event;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@DbmsExtension
class TxStateTransactionDataSnapshotIT
{

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void noPageCacheAccessOnEmptyTransactionSnapshot()
    {
        try ( Transaction transaction = database.beginTx() )
        {
            var kernelTransaction = getKernelTransaction( transaction );
            var transactionState = kernelTransaction.txState();
            var cursorTracer = kernelTransaction.pageCursorTracer();
            try ( var snapshot = new TxStateTransactionDataSnapshot( transactionState, kernelTransaction.newStorageReader(), kernelTransaction ) )
            {
                // empty
            }
            assertZeroTracer( cursorTracer );
        }
    }

    @Test
    void tracePageCacheAccessOnTransactionSnapshotCreation()
    {
        long nodeId;
        long relationshipId;
        try ( Transaction transaction = database.beginTx() )
        {
            var node1 = transaction.createNode();
            var node2 = transaction.createNode();
            var relationship = node1.createRelationshipTo( node2, RelationshipType.withName( "marker" ) );
            node1.setProperty( "foo", "bar" );
            nodeId = node1.getId();
            relationshipId = relationship.getId();
            transaction.commit();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.getNodeById( nodeId ).delete();
            transaction.getRelationshipById( relationshipId ).delete();

            var kernelTransaction = getKernelTransaction( transaction );
            var transactionState = kernelTransaction.txState();
            var cursorTracer = kernelTransaction.pageCursorTracer();
            cursorTracer.reportEvents();

            try ( var snapshot = new TxStateTransactionDataSnapshot( transactionState, kernelTransaction.newStorageReader(), kernelTransaction ) )
            {
                // no work for snapshot
            }
            assertThat( cursorTracer.pins() ).isEqualTo( 3 );
            assertThat( cursorTracer.hits() ).isEqualTo( 3 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 3 );
        }
    }

    private KernelTransactionImplementation getKernelTransaction( Transaction transaction )
    {
        return (KernelTransactionImplementation) ((InternalTransaction) transaction).kernelTransaction();
    }

    private void assertZeroTracer( PageCursorTracer cursorTracer )
    {
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.hits() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
    }
}
