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
package org.neo4j.wal.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.wal.WALService;

@SkipOnSpd(reason = "SPD doesn't support these older versions")
@Neo4jLayoutExtension
public class CheckpointKernelVersionIT {
    @Inject
    private Neo4jLayout dbLayout;

    private CheckPointer checkPointer;

    private WALService walService;

    private DatabaseManagementService dbms;

    private void setupDbms(DbmsRuntimeVersion version) {
        dbms = new TestDatabaseManagementServiceBuilder(dbLayout)
                .setConfig(Config.defaults(Map.of(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        version.getVersion(),
                        GraphDatabaseInternalSettings.latest_kernel_version,
                        version.kernelVersion().version())))
                .build();
        DependencyResolver dependencyResolver =
                ((GraphDatabaseAPI) dbms.database(DEFAULT_DATABASE_NAME)).getDependencyResolver();
        checkPointer = dependencyResolver.resolveDependency(CheckPointer.class);
        walService = dependencyResolver.resolveDependency(WALService.class);
    }

    @AfterEach
    void tearDown() {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
        }
    }

    @Test
    void checkPointRecordContainsDatabaseKernelVersion() throws IOException {
        // earlier version do not support new format of checkpoint commands; it's impossible to read them back, so we
        // cannot test them
        DbmsRuntimeVersion version = DbmsRuntimeVersion.V5_7;
        setupDbms(version);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Forced " + version.kernelVersion()));

        final var checkpoint =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpoint.kernelVersion())
                .as("kernel version from last checkpoint")
                .isEqualTo(version.kernelVersion());
    }

    @Test
    void checkPointRecordContainsAppendIndex() throws IOException {
        DbmsRuntimeVersion version = DbmsRuntimeVersion.V5_20;
        setupDbms(version);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Forced " + version.kernelVersion()));

        final var checkpoint =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertThat(checkpoint.appendIndex())
                .as("Append index should be positive number")
                .isGreaterThan(AppendIndexProvider.BASE_APPEND_INDEX);
    }

    @Test
    void checkPointInLegacy5_0Format() throws IOException {
        DbmsRuntimeVersion version = DbmsRuntimeVersion.V5_0;
        setupDbms(version);
        checkPointer.forceCheckPoint(new SimpleTriggerInfo("Legacy format."));

        final var checkpoint =
                walService.getCheckpointFile().findLatestCheckpoint().orElseThrow();
        assertEquals(KernelVersion.V5_0, checkpoint.kernelVersion());
        assertEquals(UNKNOWN_CONSENSUS_INDEX, checkpoint.transactionId().consensusIndex());
    }
}
