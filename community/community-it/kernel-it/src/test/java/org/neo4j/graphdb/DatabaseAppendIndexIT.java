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
package org.neo4j.graphdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.test.Race;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;

@DbmsExtension
public class DatabaseAppendIndexIT {
    @Inject
    private LogMetadataProvider metadataProvider;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private WALService walService;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Test
    void appendIndexAndTransactionIdsAreMatching() {
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getHighestGapFreeClosedTransactionId());

        try (Transaction transaction = databaseAPI.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getHighestGapFreeClosedTransactionId());
    }

    @Test
    void appendIndexMatchingTransactionIdWhenInternalTransactionsExecuted() {
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getHighestGapFreeClosedTransactionId());

        try (Transaction transaction = databaseAPI.beginTx()) {
            var node = transaction.createNode(Label.label("marker"));
            node.setProperty("foo", "bar");
            transaction.commit();
        }
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getHighestGapFreeClosedTransactionId());
    }

    @Test
    void checkpointContainsLastAppendedIndex() throws IOException {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var node = transaction.createNode(Label.label("marker"));
            node.setProperty("foo", "bar");
            transaction.commit();
        }

        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getHighestGapFreeClosedTransactionId());

        checkPointer.forceCheckPoint(new SimpleTriggerInfo("test trigger"));

        var checkpointInfo =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertEquals(metadataProvider.getLastAppendIndex(), checkpointInfo.appendIndex());
        assertEquals(
                metadataProvider.getHighestGapFreeClosedTransactionId(),
                checkpointInfo.transactionId().id());
    }

    @RepeatedTest(10)
    void lastCommittedBatchCorrectAfterConcurrentTransactions() throws Throwable {
        Race race = new Race();
        race.addContestants(100, () -> {
            try (Transaction tx = databaseAPI.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        });
        race.go();

        long appendIndexClosed = metadataProvider.getLastClosedBatch().appendIndex();
        long appendIndexCommitted = metadataProvider.getLastCommittedBatch().appendIndex();

        assertThat(appendIndexClosed)
                .as(
                        "Expecting last closed batch index: %s to be equal to last committed batch append index: %s",
                        appendIndexClosed, appendIndexCommitted)
                .isEqualTo(appendIndexCommitted);
    }
}
