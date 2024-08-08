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
package org.neo4j.kernel.impl.transaction;

import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.io.ByteUnit.MebiByte;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.CommandCommitListener;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class TransactionAppendIndexIT {
    private static final int MB_DATA_SIZE = 5;

    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private TransactionIdStore txIdStore;

    private BatchValidatorListener batchValidatorListener;

    private final AtomicBoolean enabledCheck = new AtomicBoolean();

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        batchValidatorListener = new BatchValidatorListener();
        builder.setExternalDependencies(dependenciesOf(new CommandCommitListeners(batchValidatorListener)));
    }

    @Test
    void transactionsAppendIndexes() {
        String propertyName = "data";
        String data = getRandomData();
        int txNumber = 20;

        try (Transaction transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty(propertyName, "a");
            transaction.commit();
        }

        enabledCheck.set(true);
        long latestAppendIndexBeforeLoop = txIdStore.getLastCommittedBatch().appendIndex();

        for (int transactions = 0; transactions < txNumber; transactions++) {
            try (Transaction transaction = database.beginTx()) {
                for (int i = 0; i < 10; i++) {
                    Node node = transaction.createNode();
                    node.setProperty(propertyName, data);
                }
                transaction.commit();
            }
        }
        enabledCheck.set(false);

        CopyOnWriteArrayList<Long> observedBatches = batchValidatorListener.getObservedBatches();
        int expectedNumberOfAppends = txNumber;

        assertThat(observedBatches).hasSize(expectedNumberOfAppends);
        assertThat(observedBatches)
                .containsExactlyElementsOf(LongStream.range(
                                latestAppendIndexBeforeLoop + 1,
                                latestAppendIndexBeforeLoop + 1 + expectedNumberOfAppends)
                        .boxed()
                        .toList());
    }

    private static String getRandomData() {
        return randomAscii((int) MebiByte.toBytes(MB_DATA_SIZE));
    }

    private class BatchValidatorListener implements CommandCommitListener {
        private final CopyOnWriteArrayList<Long> observedBatches = new CopyOnWriteArrayList<>();

        @Override
        public void onCommandBatchCommitFailure(CommandBatch commandBatch, Exception exception) {}

        @Override
        public void onCommandBatchCommitSuccess(CommandBatch commandBatch, long lastCommittedTx) {
            if (enabledCheck.get()) {
                observedBatches.add(commandBatch.appendIndex());
            }
        }

        public CopyOnWriteArrayList<Long> getObservedBatches() {
            return observedBatches;
        }
    }
}
