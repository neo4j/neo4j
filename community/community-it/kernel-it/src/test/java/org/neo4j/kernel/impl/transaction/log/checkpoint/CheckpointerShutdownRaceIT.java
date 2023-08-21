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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.CheckpointPolicy.CONTINUOUS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class CheckpointerShutdownRaceIT {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @RepeatedTest(5)
    void databaseShutdownDuringConstantCheckPointing() {
        var executor = Executors.newSingleThreadExecutor();
        DatabaseManagementService managementService = null;
        Future<?> asyncCheckpointer;
        try {
            managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                    .setFileSystem(fs)
                    .setInternalLogProvider(logProvider)
                    .build();
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
            asyncCheckpointer = executor.submit(() -> {
                try {
                    MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(100));
                    checkPointer.tryCheckPointNoWait(new SimpleTriggerInfo("test"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            assertThat(asyncCheckpointer).succeedsWithin(ofMinutes(1));
            LogAssertions.assertThat(logProvider)
                    .doesNotContainMessage("Checkpoint was requested on already shutdown checkpointer.");
        } finally {
            if (managementService != null) {
                managementService.shutdown();
            }
            executor.shutdown();
        }
    }

    @RepeatedTest(5)
    void databaseShutdownDuringContinuousCheckpointing() {
        var executor = Executors.newSingleThreadExecutor();
        DatabaseManagementService managementService = null;
        Future<?> asyncCheckpointer;
        try {
            managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                    .setFileSystem(fs)
                    .setInternalLogProvider(logProvider)
                    .setConfig(GraphDatabaseSettings.check_point_policy, CONTINUOUS)
                    .build();
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
            asyncCheckpointer = executor.submit(() -> {
                try {
                    MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(200));
                    checkPointer.tryCheckPointNoWait(new SimpleTriggerInfo("test"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertThat(asyncCheckpointer).succeedsWithin(ofMinutes(1));

            LogAssertions.assertThat(logProvider)
                    .doesNotContainMessage("Checkpoint was requested on already shutdown checkpointer.");
        } finally {
            if (managementService != null) {
                managementService.shutdown();
            }
            executor.shutdown();
        }
    }

    @Test
    void warnAboutCheckpointOnShutdownCheckpointer() throws IOException {
        DatabaseManagementService managementService = null;
        try {
            managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                    .setFileSystem(fs)
                    .setInternalLogProvider(logProvider)
                    .build();
            GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = db.beginTx()) {
                tx.createNode();
                tx.commit();
            }

            CheckPointer checkPointer = db.getDependencyResolver().resolveDependency(CheckPointer.class);
            managementService.shutdown();

            checkPointer.tryCheckPointNoWait(new SimpleTriggerInfo("test"));

            LogAssertions.assertThat(logProvider)
                    .containsMessages("Checkpoint was requested on already shutdown checkpointer.");
        } finally {
            if (managementService != null) {
                managementService.shutdown();
            }
        }
    }
}
