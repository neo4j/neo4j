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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.configuration.SettingValueParsers.FALSE;

import java.io.IOException;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFile.LogFileEventListener;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
class LogPruningIT {
    private static final SimpleTriggerInfo triggerInfo = new SimpleTriggerInfo("forced trigger");

    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private LogFiles logFiles;

    @Inject
    private CheckPointer checkPointer;

    @Inject
    private Config config;

    @Test
    void pruningStrategyShouldBeDynamic() throws IOException {
        final var deletions = LongLists.mutable.empty();
        final var rotations = LongLists.mutable.empty();
        logFiles.getLogFile().addLogFileEventListener(new LogFileEventListener() {
            @Override
            public void onDeletion(long version) {
                deletions.add(version);
            }

            @Override
            public void onRotation(LogPosition endLogPosition) {
                rotations.add(endLogPosition.getLogVersion());
            }
        });

        // Force transaction log rotation
        writeTransactionsAndRotateTwice();

        // Checkpoint to make sure strategy is evaluated
        checkPointer.forceCheckPoint(triggerInfo);

        // Make sure file is still there since we have disabled pruning.
        // 2 transaction logs and 1 separate checkpoint file.
        assertThat(countTransactionLogs(logFiles)).isEqualTo(4);

        // Change pruning to true
        config.setDynamic(keep_logical_logs, FALSE, "LogPruningIT");

        // Checkpoint to make sure strategy is evaluated
        checkPointer.forceCheckPoint(triggerInfo);

        // Make sure file is removed
        assertThat(countTransactionLogs(logFiles)).isEqualTo(2);
        assertThat(rotations.toArray())
                .as("should have tracked the 2 log rotations")
                .containsExactly(0L, 1L);
        assertThat(deletions.toArray())
                .as("should have tracked the 2 log prunes")
                .containsExactly(0L, 1L);
    }

    private void writeTransactionsAndRotateTwice() throws IOException {
        // Apparently we always keep an extra log file what even though the threshold is reached... produce two then
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        logFiles.getLogFile().getLogRotation().rotateLogFile(LogAppendEvent.NULL);
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        logFiles.getLogFile().getLogRotation().rotateLogFile(LogAppendEvent.NULL);
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
    }

    private static int countTransactionLogs(LogFiles logFiles) throws IOException {
        return logFiles.logFiles().length;
    }
}
