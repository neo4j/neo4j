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
package org.neo4j.db;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilderImplementation;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class DatabaseStartupTest {
    @Inject
    FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neoLayout;

    @Test
    void startDatabaseWithWrongVersionShouldFail() throws Throwable {
        // given
        // create a store
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(neoLayout).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        var storageEngineFactory = db.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        DatabaseLayout databaseLayout = db.databaseLayout();
        managementService.shutdown();

        // when messing up the version in the meta-data store
        tamperWithMetaDataStore(storageEngineFactory, databaseLayout, metadataProvider -> {
            var originalId = metadataProvider.getStoreId();
            metadataProvider.regenerateMetadata(
                    new StoreId(originalId.getCreationTime(), originalId.getRandom(), "bad", "even_worse", 1, 1),
                    UUID.randomUUID(),
                    NULL_CONTEXT);
        });

        // then
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        GraphDatabaseAPI databaseService = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        try {

            assertThrows(DatabaseShutdownException.class, databaseService::beginTx);
            DatabaseStateService dbStateService =
                    databaseService.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            assertTrue(
                    dbStateService.causeOfFailure(databaseService.databaseId()).isPresent());
            assertThat(dbStateService
                            .causeOfFailure(databaseService.databaseId())
                            .get())
                    .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class)
                    .hasRootCauseMessage("Unknown store version 'bad-even_worse-1.1'");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void startDatabaseWithWrongTransactionFilesShouldFail() throws Exception {
        // Create a store
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(neoLayout).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        DatabaseLayout databaseLayout = db.databaseLayout();
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        var storageEngineFactory = db.getDependencyResolver().resolveDependency(StorageEngineFactory.class);
        managementService.shutdown();

        // Change store id component
        tamperWithMetaDataStore(storageEngineFactory, databaseLayout, metadataProvider -> {
            var originalId = metadataProvider.getStoreId();
            var newStoreId = new StoreId(
                    System.currentTimeMillis() + 1,
                    originalId.getRandom(),
                    originalId.getStorageEngineName(),
                    originalId.getFormatName(),
                    originalId.getMajorVersion(),
                    originalId.getMinorVersion());
            metadataProvider.regenerateMetadata(newStoreId, UUID.randomUUID(), NULL_CONTEXT);
        });

        // Try to start
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        try {
            db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            assertFalse(db.isAvailable(10));

            DatabaseStateService dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            Optional<Throwable> cause = dbStateService.causeOfFailure(db.databaseId());
            assertTrue(cause.isPresent());
            assertTrue(cause.get().getCause().getMessage().contains("Mismatching store id"));
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void startDatabaseWithoutStoreFilesAndWithTransactionLogFilesFailure() throws IOException {
        // Create a store
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(neoLayout).build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
        DatabaseLayout databaseLayout = db.databaseLayout();
        try (Transaction tx = db.beginTx()) {
            tx.createNode();
            tx.commit();
        }
        managementService.shutdown();

        fs.deleteRecursively(databaseLayout.databaseDirectory());

        // Try to start
        managementService = new TestDatabaseManagementServiceBuilder(databaseLayout).build();
        try {
            db = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
            assertFalse(db.isAvailable(10));

            DatabaseStateService dbStateService =
                    db.getDependencyResolver().resolveDependency(DatabaseStateService.class);
            Optional<Throwable> cause = dbStateService.causeOfFailure(db.databaseId());
            assertTrue(cause.isPresent());
            assertThat(cause.get())
                    .hasStackTraceContaining("Fail to start '" + db.databaseId()
                            + "' since transaction logs were found, while database ");
        } finally {
            managementService.shutdown();
        }
    }

    @Test
    void startTestDatabaseOnProvidedNonAbsoluteFile() {
        Path directory = Path.of("target/notAbsoluteDirectory");
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(directory)
                .impermanent()
                .build();
        managementService.shutdown();
    }

    @Test
    void startCommunityDatabaseOnProvidedNonAbsoluteFile() {
        Path directory = Path.of("target/notAbsoluteDirectory");
        EphemeralCommunityManagementServiceFactory factory = new EphemeralCommunityManagementServiceFactory();
        EphemeralDatabaseManagementServiceBuilder databaseFactory =
                new EphemeralDatabaseManagementServiceBuilder(directory, factory);
        DatabaseManagementService managementService = databaseFactory.build();
        managementService.database(DEFAULT_DATABASE_NAME);
        managementService.shutdown();
    }

    @Test
    void dumpSystemDiagnosticLoggingOnStartup() {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder(neoLayout)
                .setInternalLogProvider(logProvider)
                .setConfig(GraphDatabaseInternalSettings.dump_diagnostics, true)
                .build();
        managementService.database(DEFAULT_DATABASE_NAME);
        try {
            assertThat(logProvider)
                    .containsMessages(
                            "System diagnostics",
                            "System memory information",
                            "JVM memory information",
                            "Operating system information",
                            "JVM information",
                            "Java classpath",
                            "Library path",
                            "System properties",
                            "(IANA) TimeZone database version",
                            "Network information",
                            "DBMS config");
        } finally {
            managementService.shutdown();
        }
    }

    private void tamperWithMetaDataStore(
            StorageEngineFactory storageEngineFactory, DatabaseLayout databaseLayout, Consumer<MetadataProvider> tamper)
            throws Exception {
        try (var scheduler = JobSchedulerFactory.createInitialisedScheduler();
                var pageCache = new MuninnPageCache(
                        new SingleFilePageSwapperFactory(fs, NULL, INSTANCE),
                        scheduler,
                        MuninnPageCache.config(1_000));
                var metadataProvider = storageEngineFactory.transactionMetaDataStore(
                        fs,
                        databaseLayout,
                        Config.defaults(),
                        pageCache,
                        writable(),
                        NULL_CONTEXT_FACTORY,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        NULL)) {
            tamper.accept(metadataProvider);
        }
    }

    private static class EphemeralCommunityManagementServiceFactory extends DatabaseManagementServiceFactory {
        EphemeralCommunityManagementServiceFactory() {
            super(DbmsInfo.COMMUNITY, CommunityEditionModule::new);
        }

        @Override
        protected GlobalModule createGlobalModule(
                Config config, boolean daemonMode, ExternalDependencies dependencies) {
            return new GlobalModule(config, dbmsInfo, daemonMode, dependencies) {
                @Override
                protected FileSystemAbstraction createFileSystemAbstraction() {
                    return new EphemeralFileSystemAbstraction();
                }
            };
        }
    }

    private static class EphemeralDatabaseManagementServiceBuilder
            extends DatabaseManagementServiceBuilderImplementation {
        private final EphemeralCommunityManagementServiceFactory factory;

        EphemeralDatabaseManagementServiceBuilder(
                Path homeDirectory, EphemeralCommunityManagementServiceFactory factory) {
            super(homeDirectory);
            this.factory = factory;
        }

        @Override
        protected DatabaseManagementService newDatabaseManagementService(
                Config config, ExternalDependencies dependencies) {
            return factory.build(augmentConfig(config), daemonMode, dependencies);
        }
    }
}
