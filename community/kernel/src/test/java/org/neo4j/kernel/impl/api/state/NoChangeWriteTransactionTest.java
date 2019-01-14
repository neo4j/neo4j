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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.index.IndexManager.PROVIDER;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.index.DummyIndexExtensionFactory.IDENTIFIER;

public class NoChangeWriteTransactionTest
{
    @Rule
    public final DatabaseRule dbr = new ImpermanentDatabaseRule();

    @Test
    public void shouldIdentifyTransactionWithNetZeroChangesAsReadOnly()
    {
        // GIVEN a transaction that has seen some changes, where all those changes result in a net 0 change set
        // a good way of producing such state is to add a label to an existing node, and then remove it.
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        long startTxId = txIdStore.getLastCommittedTransactionId();
        Node node = createEmptyNode( db );
        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( TestLabels.LABEL_ONE );
            node.removeLabel( TestLabels.LABEL_ONE );
            tx.success();
        } // WHEN closing that transaction

        // THEN it should not have been committed
        assertEquals( "Expected last txId to be what it started at + 2 (1 for the empty node, and one for the label)",
                startTxId + 2, txIdStore.getLastCommittedTransactionId() );
    }

    @Test
    public void shouldDetectNoChangesInCommitsAlsoForTheIndexes()
    {
        // GIVEN a transaction that has seen some changes, where all those changes result in a net 0 change set
        // a good way of producing such state is to add a label to an existing node, and then remove it.
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        TransactionIdStore txIdStore = db.getDependencyResolver().resolveDependency( TransactionIdStore.class );
        long startTxId = txIdStore.getLastCommittedTransactionId();
        Node node = createEmptyNode( db );
        Index<Node> index = createNodeIndex( db );
        try ( Transaction tx = db.beginTx() )
        {
            node.addLabel( TestLabels.LABEL_ONE );
            node.removeLabel( TestLabels.LABEL_ONE );
            index.add( node, "key", "value" );
            index.remove( node, "key", "value" );
            tx.success();
        } // WHEN closing that transaction

        // THEN it should not have been committed
        assertEquals( "Expected last txId to be what it started at + 3 " +
                      "(1 for the empty node, 1 for index, and one for the label)",
                startTxId + 3, txIdStore.getLastCommittedTransactionId() );
    }

    private Index<Node> createNodeIndex( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( "test", stringMap( PROVIDER, IDENTIFIER ) );
            tx.success();
            return index;
        }
    }

    private Node createEmptyNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.success();
            return node;
        }
    }

}
