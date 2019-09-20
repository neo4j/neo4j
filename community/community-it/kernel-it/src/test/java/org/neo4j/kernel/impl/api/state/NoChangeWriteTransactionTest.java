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
package org.neo4j.kernel.impl.api.state;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ImpermanentDbmsExtension
class NoChangeWriteTransactionTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldIdentifyTransactionWithNetZeroChangesAsReadOnly()
    {
        // GIVEN a transaction that has seen some changes, where all those changes result in a net 0 change set
        // a good way of producing such state is to add a label to an existing node, and then remove it.
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        long startTxId = txIdStore.getLastCommittedTransactionId();
        Node node = createEmptyNode( db );
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.getNodeById( node.getId() );
            node.addLabel( TestLabels.LABEL_ONE );
            node.removeLabel( TestLabels.LABEL_ONE );
            tx.commit();
        } // WHEN closing that transaction

        // THEN it should not have been committed
        assertEquals( startTxId + 2, txIdStore.getLastCommittedTransactionId(),
                "Expected last txId to be what it started at + 2 (1 for the empty node, and one for the label)" );
    }

    private Node createEmptyNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            tx.commit();
            return node;
        }
    }
}
