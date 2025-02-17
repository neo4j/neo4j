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
package org.neo4j.kernel.recovery;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.do_parallel_recovery;
import static org.neo4j.kernel.recovery.RecoveryHelpers.removeLastCheckpointRecordFromLogFile;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class ParallelRecoveryIT extends RecoveryIT {
    @Override
    void additionalConfiguration(Config config) {
        super.additionalConfiguration(config);
        config.set(do_parallel_recovery, true);
    }

    @Override
    TestDatabaseManagementServiceBuilder additionalConfiguration(TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(do_parallel_recovery, true);
    }

    @Test
    void indexDropTogetherWithIndexUpdatesInParallelRecovery() throws Throwable {
        GraphDatabaseAPI database = createDatabase();
        int numberOfNodes = 10;
        Label label = Label.label("myLabel");
        String property = "prop";
        String rangeIndex = "range index";

        try (Transaction transaction = database.beginTx()) {
            transaction
                    .schema()
                    .indexFor(label)
                    .on(property)
                    .withIndexType(IndexType.RANGE)
                    .withName(rangeIndex)
                    .create();
            transaction.commit();
        }
        awaitIndexesOnline(database);

        database.getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("forced checkpoint"));

        for (int i = 0; i < numberOfNodes; i++) {
            try (Transaction tx = database.beginTx()) {
                Node node = tx.createNode(label);
                node.setProperty(property, "value");
                tx.commit();
            }
        }

        // Store a copy of the index in a temp location.
        // This might seem a bit contrived that the index must be present even though it was dropped - but it
        // is the case for example if the drop was in a diff backup and this recovery is during aggregate of backups.
        Path indexPath;
        Path temp = dir.homePath().resolve("temp");
        try (Transaction tx = database.beginTx()) {
            IndexDescriptor index = ((IndexDefinitionImpl) tx.schema().getIndexByName(rangeIndex)).getIndexReference();
            indexPath = IndexDirectoryStructure.directoriesByProvider(databaseLayout.databaseDirectory())
                    .forProvider(index.getIndexProvider())
                    .directoryForIndex(index.getId());
            fileSystem.mkdir(temp);
            fileSystem.copyRecursively(indexPath, temp);
        }

        try (Transaction tx = database.beginTx()) {
            tx.schema().getIndexByName(rangeIndex).drop();
            tx.commit();
        }
        managementService.shutdown();
        removeLastCheckpointRecordFromLogFile(databaseLayout, fileSystem);

        // Restore the index to get an OnlineIndexProxy during recovery
        fileSystem.mkdir(indexPath);
        fileSystem.copyRecursively(temp, indexPath);

        // The index updates should been done before drop and recovery should succeed
        assertDoesNotThrow(() -> recoverDatabase());
    }
}
