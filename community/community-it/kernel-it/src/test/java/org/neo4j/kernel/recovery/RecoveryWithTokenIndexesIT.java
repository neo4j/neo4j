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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.kernel.recovery.Recovery.performRecovery;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.IndexingTestUtil;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class RecoveryWithTokenIndexesIT {
    @RegisterExtension
    static final PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;
    private Config config;

    private static final Label label = Label.label("label");
    private static final RelationshipType type = RelationshipType.withName("type");

    @AfterEach
    void tearDown() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of("indexes created during recovery", false),
                Arguments.of("indexes updated during recovery", true));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("arguments")
    void recoverDatabaseWithTokenIndexes(String name, boolean checkpointIndexes) throws Throwable {
        config = Config.newBuilder().set(neo4j_home, testDirectory.homePath()).build();

        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        GraphDatabaseService db = startDatabase(fs);
        IndexingTestUtil.assertOnlyDefaultTokenIndexesExists(db);

        if (checkpointIndexes) {
            // Checkpoint to not make index creation part of the recovery.
            checkPoint(db);
        }

        int numberOfEntities = 10;
        for (int i = 0; i < numberOfEntities; i++) {
            createEntities(db);
        }

        // Don't flush/checkpoint before taking the snapshot, to make the indexes need to recover (clean crash
        // generation)
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        managementService.shutdown();
        fs.close();

        try (PageCache cache = pageCacheExtension.getPageCache(crashedFs)) {
            DatabaseLayout layout = DatabaseLayout.of(config);
            recoverDatabase(layout, crashedFs, cache);
        }

        db = startDatabase(crashedFs);

        // Verify that the default token indexes still exist
        IndexingTestUtil.assertOnlyDefaultTokenIndexesExists(db);
        awaitIndexesOnline(db);

        try (Transaction tx = db.beginTx()) {
            assertEquals(numberOfEntities, Iterators.count(tx.findNodes(label)));
            assertEquals(numberOfEntities, Iterators.count(tx.findRelationships(type)));
        }
    }

    private static void checkPoint(GraphDatabaseService db) throws IOException {
        ((GraphDatabaseAPI) db)
                .getDependencyResolver()
                .resolveDependency(CheckPointer.class)
                .forceCheckPoint(new SimpleTriggerInfo("Manual trigger"));
    }

    private static void awaitIndexesOnline(GraphDatabaseService database) {
        try (Transaction transaction = database.beginTx()) {
            transaction.schema().awaitIndexesOnline(10, MINUTES);
            transaction.commit();
        }
    }

    private GraphDatabaseService startDatabase(EphemeralFileSystemAbstraction fs) {
        managementService = new TestDatabaseManagementServiceBuilder(testDirectory.homePath())
                .setFileSystem(fs)
                .setConfig(config)
                .build();
        return managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void recoverDatabase(DatabaseLayout layout, FileSystemAbstraction fs, PageCache cache) throws Exception {
        assertTrue(Recovery.isRecoveryRequired(fs, layout, config, INSTANCE));
        performRecovery(Recovery.context(
                fs,
                cache,
                DatabaseTracers.EMPTY,
                config,
                layout,
                INSTANCE,
                IOController.DISABLED,
                NullLogProvider.getInstance(),
                LatestVersions.LATEST_KERNEL_VERSION_PROVIDER));
        assertFalse(Recovery.isRecoveryRequired(fs, layout, config, INSTANCE));
    }

    private static void createEntities(GraphDatabaseService service) {
        try (Transaction transaction = service.beginTx()) {
            Node node = transaction.createNode(label);
            node.createRelationshipTo(node, type);
            transaction.commit();
        }
    }
}
