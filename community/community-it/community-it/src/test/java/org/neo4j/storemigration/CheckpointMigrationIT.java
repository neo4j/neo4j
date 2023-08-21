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
package org.neo4j.storemigration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.ZippedStoreCommunity;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
public class CheckpointMigrationIT {
    @Inject
    protected TestDirectory directory;

    @Inject
    protected Neo4jLayout layout;

    public static List<ZippedStore> legacyStores() {
        return List.of(
                ZippedStoreCommunity.REC_SF11_V50_EMPTY,
                ZippedStoreCommunity.REC_SF11_V50_ALL,
                ZippedStoreCommunity.REC_AF11_V50_ALL,
                ZippedStoreCommunity.REC_AF11_V50_EMPTY);
    }

    @ParameterizedTest
    @MethodSource("legacyStores")
    void checkpointDatabaseWithLegacyKernelVersion(ZippedStore zippedStore) throws IOException {
        Path homeDir = layout.homeDirectory();
        zippedStore.unzip(homeDir);
        DatabaseManagementService dbms = new TestDatabaseManagementServiceBuilder(homeDir).build();
        try {
            GraphDatabaseAPI database = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
            forceCheckpoint(database);

            var latestCheckpoint = database.getDependencyResolver()
                    .resolveDependency(LogFiles.class)
                    .getCheckpointFile()
                    .findLatestCheckpoint()
                    .orElseThrow();
            assertEquals(KernelVersion.V5_0, latestCheckpoint.kernelVersion());
        } finally {
            dbms.shutdown();
        }
    }

    private static void forceCheckpoint(GraphDatabaseAPI database) throws IOException {
        database.getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("test"));
    }
}
