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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.VOLUME;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_interval_volume;
import static org.neo4j.configuration.GraphDatabaseSettings.check_point_policy;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class VolumeBasedCheckpointIT {
    private static final int WAIT_TIMEOUT_MINUTES = 10;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService dbms;

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @Test
    void checkpointOnVolumeThresholdSingleLogFile() {
        Config volumeCheckpointConfig = Config.newBuilder()
                .set(check_point_policy, VOLUME)
                .set(check_point_interval_volume, kibiBytes(100))
                .build();
        dbms = startDbms(volumeCheckpointConfig);
        GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
        var checkPointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var lastCheckpointedTransactionId = checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId();

        try (var transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty("a", randomAscii((int) kibiBytes(100)));
            transaction.commit();
        }

        await().atMost(ofSeconds(WAIT_TIMEOUT_MINUTES)).untilAsserted(() -> assertThat(checkPointer
                        .latestCheckPointInfo()
                        .highestObservedClosedTransactionId()
                        .id())
                .isGreaterThan(lastCheckpointedTransactionId.id()));
    }

    @Test
    void checkpointOnVolumeThresholdMultipleLogFiles() {
        Config volumeCheckpointConfig = Config.newBuilder()
                .set(check_point_policy, VOLUME)
                .set(check_point_interval_volume, mebiBytes(2))
                .set(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(128))
                .build();

        dbms = startDbms(volumeCheckpointConfig);
        GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
        var checkPointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var lastCheckpointedTransactionId = checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId();

        for (int i = 0; i < 1024; i++) {
            try (var transaction = database.beginTx()) {
                Node node = transaction.createNode();
                node.setProperty("a", randomAscii((int) kibiBytes(128)));
                transaction.commit();
            }
        }

        await().atMost(ofMinutes(WAIT_TIMEOUT_MINUTES)).untilAsserted(() -> assertThat(checkPointer
                        .latestCheckPointInfo()
                        .highestObservedClosedTransactionId()
                        .id())
                .isGreaterThan(lastCheckpointedTransactionId.id()));
    }

    @Test
    void checkpointOnVolumeSplitBetweenLogFiles() {
        Config volumeCheckpointConfig = Config.newBuilder()
                .set(check_point_policy, VOLUME)
                .set(check_point_interval_volume, kibiBytes(180))
                .set(GraphDatabaseSettings.logical_log_rotation_threshold, kibiBytes(128))
                .build();

        dbms = startDbms(volumeCheckpointConfig);
        GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME);
        var checkPointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var lastCheckpointedTransactionId = checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId();

        try (var transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty("a", randomAscii((int) kibiBytes(128)));
            transaction.commit();
        }
        try (var transaction = database.beginTx()) {
            Node node = transaction.createNode();
            node.setProperty("a", randomAscii((int) kibiBytes(64)));
            transaction.commit();
        }

        await().atMost(ofSeconds(WAIT_TIMEOUT_MINUTES)).untilAsserted(() -> assertThat(checkPointer
                        .latestCheckPointInfo()
                        .highestObservedClosedTransactionId()
                        .id())
                .isGreaterThan(lastCheckpointedTransactionId.id()));
    }

    private DatabaseManagementService startDbms(Config config) {
        return new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setConfig(config)
                .build();
    }
}
