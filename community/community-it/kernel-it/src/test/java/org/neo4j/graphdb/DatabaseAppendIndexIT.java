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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class DatabaseAppendIndexIT {
    @Inject
    private MetadataProvider metadataProvider;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private LogFiles logFiles;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Test
    void appendIndexAndTransactionIdsAreMatching() {
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getLastClosedTransactionId());

        try (Transaction transaction = databaseAPI.beginTx()) {
            transaction.createNode();
            transaction.commit();
        }
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getLastClosedTransactionId());
    }

    @Test
    void appendIndexMatchingTransactionIdWhenInternalTransactionsExecuted() {
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getLastClosedTransactionId());

        try (Transaction transaction = databaseAPI.beginTx()) {
            var node = transaction.createNode(Label.label("marker"));
            node.setProperty("foo", "bar");
            transaction.commit();
        }
        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getLastClosedTransactionId());
    }

    @Test
    void checkpointContainsLastAppendedIndex() throws IOException {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var node = transaction.createNode(Label.label("marker"));
            node.setProperty("foo", "bar");
            transaction.commit();
        }

        assertEquals(metadataProvider.getLastAppendIndex(), metadataProvider.getLastClosedTransactionId());

        checkPointer.forceCheckPoint(new SimpleTriggerInfo("test trigger"));

        var checkpointInfo = logFiles.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertEquals(metadataProvider.getLastAppendIndex(), checkpointInfo.appendIndex());
        assertEquals(
                metadataProvider.getLastClosedTransactionId(),
                checkpointInfo.transactionId().id());
    }
}
