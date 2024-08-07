/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.command;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.singleOrNull;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * This test is for an issue with transaction batching where there would be a batch of transactions
 * to be applied in the same batch; the batch containing a creation of node N with label L and property P.
 * Later in that batch there would be a uniqueness constraint created for label L and property P.
 * The number of nodes matching this constraint would be few and so the label scan store would be selected
 * to drive the population of the index. Problem is that the label update for N would still sit in
 * the batch state, to be applied at the end of the batch. Hence, the node would be forgotten when the
 * index was being built.
 */
class LabelAndIndexUpdateBatchingIT {
    private static final String PROPERTY_KEY = "key";
    private static final Label LABEL = Label.label("label");

    @Test
    void indexShouldIncludeNodesCreatedPreviouslyInBatch() throws Exception {
        // GIVEN a transaction stream leading up to this issue
        // perform the transactions from db-level and extract the transactions as commands
        // so that they can be applied batch-wise they way we'd like to later.

        List<CommittedCommandBatchRepresentation> transactions;
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        // We don't want to include any transactions that has been run on start-up when applying to the new database
        // later.
        long txIdToStartFrom = getLastClosedTransactionId(db) + 1;
        long txIdCutOffPoint;

        // a bunch of nodes (to have the index population later on to decide to use label scan for population)
        String nodeN = "our guy";
        String otherNode = "just to create the tokens";
        try {
            try (Transaction tx = db.beginTx()) {
                tx.createNode(LABEL).setProperty(PROPERTY_KEY, otherNode);
                for (int i = 0; i < 10_000; i++) {
                    tx.createNode();
                }
                tx.commit();
            }
            // node N
            try (Transaction tx = db.beginTx()) {
                tx.createNode(LABEL).setProperty(PROPERTY_KEY, nodeN);
                tx.commit();
            }
            txIdCutOffPoint = db.getDependencyResolver()
                    .resolveDependency(TransactionIdStore.class)
                    .getLastClosedTransactionId();
            // uniqueness constraint affecting N
            try (Transaction tx = db.beginTx()) {
                tx.schema()
                        .constraintFor(LABEL)
                        .assertPropertyIsUnique(PROPERTY_KEY)
                        .create();
                tx.commit();
            }
            transactions = extractTransactions(db, txIdToStartFrom);
        } finally {
            managementService.shutdown();
        }

        managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        TransactionCommitProcess commitProcess =
                db.getDependencyResolver().resolveDependency(TransactionCommitProcess.class);
        try {
            int cutoffIndex = findCutoffIndex(transactions, txIdCutOffPoint);
            commitProcess.commit(
                    toApply(transactions.subList(0, cutoffIndex), db), TransactionWriteEvent.NULL, EXTERNAL);

            // WHEN applying the two transactions (node N and the constraint) in the same batch
            commitProcess.commit(
                    toApply(transactions.subList(cutoffIndex, transactions.size()), db),
                    TransactionWriteEvent.NULL,
                    EXTERNAL);

            // THEN node N should've ended up in the index too
            try (Transaction tx = db.beginTx()) {
                assertNotNull(
                        singleOrNull(tx.findNodes(LABEL, PROPERTY_KEY, otherNode)),
                        "Verification node not found"); // just to verify
                assertNotNull(singleOrNull(tx.findNodes(LABEL, PROPERTY_KEY, nodeN)), "Node N not found");
                tx.commit();
            }
        } finally {
            managementService.shutdown();
        }
    }

    private static int findCutoffIndex(Collection<CommittedCommandBatchRepresentation> transactions, long txId) {
        var iterator = transactions.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            var tx = iterator.next();
            if (tx.txId() == txId) {
                return i;
            }
        }
        throw new AssertionError("Couldn't find the transaction which would be the cut-off point");
    }

    private static CompleteTransaction toApply(
            Collection<CommittedCommandBatchRepresentation> transactions, GraphDatabaseAPI db) {
        StorageEngine storageEngine = db.getDependencyResolver().resolveDependency(StorageEngine.class);
        var commitmentFactory = db.getDependencyResolver().resolveDependency(TransactionCommitmentFactory.class);
        var transactionIdGenerator = db.getDependencyResolver().resolveDependency(TransactionIdGenerator.class);
        CompleteTransaction first = null;
        CompleteTransaction last = null;
        try (var storeCursors = storageEngine.createStorageCursors(NULL_CONTEXT)) {
            for (var tx : transactions) {
                var transaction = new CompleteTransaction(
                        tx, NULL_CONTEXT, storeCursors, commitmentFactory.newCommitment(), transactionIdGenerator);
                if (first == null) {
                    first = last = transaction;
                } else {
                    last.next(transaction);
                    last = transaction;
                }
            }
        }
        return first;
    }

    private static List<CommittedCommandBatchRepresentation> extractTransactions(
            GraphDatabaseAPI db, long txIdToStartOn) throws IOException {
        LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency(LogicalTransactionStore.class);
        List<CommittedCommandBatchRepresentation> transactions = new ArrayList<>();
        try (CommandBatchCursor cursor = txStore.getCommandBatches(txIdToStartOn)) {
            cursor.forAll(transactions::add);
        }
        return transactions;
    }

    private static long getLastClosedTransactionId(GraphDatabaseAPI database) {
        MetadataProvider metaDataStore = database.getDependencyResolver().resolveDependency(MetadataProvider.class);
        return metaDataStore.getLastClosedTransaction().transactionId().id();
    }
}
