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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.include_versions_under_development;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.driver.internal.util.Iterables.count;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.schema.IndexType.RANGE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.DbStatistics;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.ZippedStore;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
public abstract class DatabaseMigrationITBase {
    @Inject
    protected TestDirectory directory;

    @Inject
    protected Neo4jLayout layout;

    protected abstract TestDatabaseManagementServiceBuilder newDbmsBuilder(Path homeDir);

    protected abstract void verifySystemDbSchema(GraphDatabaseService system, SystemDbMigration systemDbMigration);

    protected void migrateOrRemoveSystemDatabase(ZippedStore zippedStore, Neo4jLayout layout) throws IOException {
        var result = StoreMigrationTestUtils.runStoreMigrationCommandFromSameJvm(layout, SYSTEM_DATABASE_NAME);
        assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);
    }

    protected StoreMigrationTestUtils.Result migrate(Neo4jLayout neo4jLayout, String... args) {
        return StoreMigrationTestUtils.runStoreMigrationCommandFromSameJvm(neo4jLayout, args);
    }

    protected void doShouldMigrateDatabase(ZippedStore zippedStore, String toRecordFormat, boolean includeExperimental)
            throws IOException, ConsistencyCheckIncompleteException {
        // given
        Path homeDir = layout.homeDirectory();
        zippedStore.unzip(homeDir);

        String[] args = {"--to-format", toRecordFormat, "--verbose", DEFAULT_DATABASE_NAME};
        if (includeExperimental) {
            Path additionalConfig = directory.file("add-config.conf");
            Files.writeString(additionalConfig, include_versions_under_development.name() + "=true");
            args = ArrayUtils.addAll(args, "--additional-config", additionalConfig.toString());
        }

        // when
        StoreMigrationTestUtils.Result result = migrate(layout, args);
        assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);

        migrateOrRemoveSystemDatabase(zippedStore, layout);

        // then
        TestDatabaseManagementServiceBuilder builder = newDbmsBuilder(homeDir);
        if (includeExperimental) {
            builder.setConfig(include_versions_under_development, true);
            builder.setConfig(GraphDatabaseSettings.db_format, toRecordFormat);
        }
        DatabaseManagementService dbms = builder.build();

        try {
            GraphDatabaseService db = dbms.database(DEFAULT_DATABASE_NAME);
            verifyContents(db, zippedStore.statistics(), toRecordFormat);
            verifyStoreFormat(db, expectedFormat(db, toRecordFormat));
            verifyTokenIndexes(db);
            verifyKernelVersion(db);
            verifyRemovedIndexProviders(db);
            verifyFulltextIndexes(db, zippedStore.statistics().kernelVersion());
        } finally {
            dbms.shutdown();
        }
        // for now we skip index check for experimental formats (multiversion only atm)
        consistencyCheck(homeDir, DEFAULT_DATABASE_NAME, includeExperimental);
    }

    protected StoreVersionIdentifier expectedFormat(GraphDatabaseService db, String toRecordFormat) {
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        DependencyResolver dependencyResolver = api.getDependencyResolver();
        Config config = dependencyResolver.resolveDependency(Config.class);
        StorageEngineFactory storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
        StoreVersionCheck storeVersionCheck = storageEngineFactory.versionCheck(
                directory.getFileSystem(),
                api.databaseLayout(),
                config,
                dependencyResolver.resolveDependency(PageCache.class),
                NullLogService.getInstance(),
                CursorContextFactory.NULL_CONTEXT_FACTORY);
        return storeVersionCheck.findLatestVersion(toRecordFormat);
    }

    protected void doShouldMigrateSystemDatabaseAndOthers(SystemDbMigration systemDbMigration)
            throws IOException, ConsistencyCheckIncompleteException {
        doShouldMigrateSystemDatabase(systemDbMigration, (neo4jLayout) -> {
            StoreMigrationTestUtils.Result result = migrate(neo4jLayout, "--verbose", "*");
            assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);
        });
    }

    protected void doShouldMigrateSystemDatabase(SystemDbMigration systemDbMigration)
            throws IOException, ConsistencyCheckIncompleteException {
        doShouldMigrateSystemDatabase(systemDbMigration, (neo4jLayout) -> {
            StoreMigrationTestUtils.Result result = migrate(neo4jLayout, "--verbose", DEFAULT_DATABASE_NAME);
            assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);
            result = migrate(neo4jLayout, "--verbose", SYSTEM_DATABASE_NAME);
            assertThat(result.exitCode()).withFailMessage(result.err()).isEqualTo(0);
        });
    }

    protected void doShouldMigrateSystemDatabase(SystemDbMigration systemDbMigration, Consumer<Neo4jLayout> migrate)
            throws IOException, ConsistencyCheckIncompleteException {
        // given
        Path targetDirectory = directory.homePath();
        var neo4jLayout = Neo4jLayout.of(targetDirectory);
        systemDbMigration.zippedStore.unzip(targetDirectory);

        // when
        migrate.accept(neo4jLayout);

        var initialIndexStateMonitor = new InitialIndexStateMonitor(SYSTEM_DATABASE_NAME);
        DatabaseManagementService dbms = newDbmsBuilder(targetDirectory)
                .setConfig(automatic_upgrade_enabled, false)
                .setMonitors(initialIndexStateMonitor.monitors())
                .build();

        // then
        try {
            var system = dbms.database(SYSTEM_DATABASE_NAME);
            verifyInitialIndexState(initialIndexStateMonitor);
            verifyGraphComponents(system);
            verifyTokenIndexes(system);
            verifyKernelVersion(system);
            // In schema store migration for system db all btree indexes are replaced by range indexes.
            // Any existing indexes keep their ids and any indexes created in the migration should get new ones
            // If all the indexes made it then we know that they got unique ids - otherwise they would have overwritten
            // each other
            verifySystemDbSchema(system, systemDbMigration);
            verifyRemovedIndexProviders(system);
            verifyFulltextIndexes(
                    system, systemDbMigration.zippedStore.statistics().kernelVersion());

            // Try to do something against neo4j to see that system db still correctly references it
            var neo4j = dbms.database(DEFAULT_DATABASE_NAME);
            try (Transaction tx = neo4j.beginTx()) {
                tx.createNode();
                tx.commit();
            }
        } finally {
            dbms.shutdown();
        }
        consistencyCheck(targetDirectory, SYSTEM_DATABASE_NAME, true);
    }

    protected void verifyContents(GraphDatabaseService db, DbStatistics statistics, String toFormat) {
        verifyContents(
                db,
                statistics,
                expectedIndexesAfterUpgrade(statistics, toFormat),
                expectedConstraintsAfterUpgrade(statistics));
    }

    protected void verifyContents(
            GraphDatabaseService db,
            DbStatistics statistics,
            int expectedIndexesAfterUpgrade,
            int expectedConstraintsAfterUpgrade) {
        // then
        try (Transaction tx = db.beginTx()) {
            MutableInt numberOfNodes = new MutableInt();
            MutableInt numberOfNodeProperties = new MutableInt();
            MutableInt numberOfRelationships = new MutableInt();
            MutableInt numberOfRelationshipProperties = new MutableInt();

            Iterables.forEach(tx.getAllNodes(), node -> {
                numberOfNodeProperties.add(node.getAllProperties().size());
                numberOfNodes.increment();
            });
            Iterables.forEach(tx.getAllRelationships(), relationship -> {
                numberOfRelationshipProperties.add(
                        relationship.getAllProperties().size());
                numberOfRelationships.increment();
            });
            int numberOfSchemaIndexes = count(tx.schema().getIndexes());
            int numberOfConstraints = count(tx.schema().getConstraints());

            assertThat(numberOfNodes.intValue()).isEqualTo(statistics.nodes());
            assertThat(numberOfNodeProperties.intValue()).isEqualTo(statistics.nodeProperties());
            assertThat(numberOfRelationships.intValue()).isEqualTo(statistics.relationships());
            assertThat(numberOfRelationshipProperties.intValue()).isEqualTo(statistics.relationshipProperties());
            assertThat(numberOfSchemaIndexes).isEqualTo(expectedIndexesAfterUpgrade);
            assertThat(numberOfConstraints).isEqualTo(expectedConstraintsAfterUpgrade);

            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            tx.commit();
        }
    }

    private static int expectedIndexesAfterUpgrade(DbStatistics statistics, String toFormat) {
        return statistics.indexes() + additionalIndexAfterUpgrade(statistics, toFormat) - statistics.btreeIndexes();
    }

    private static int expectedConstraintsAfterUpgrade(DbStatistics statistics) {
        return statistics.constraints() - statistics.btreeConstraints();
    }

    private static int additionalIndexAfterUpgrade(DbStatistics statistics, String toFormat) {
        boolean acrossEngineMigration =
                toFormat.equals("block") != statistics.storeVersion().contains("block");
        if (acrossEngineMigration) {
            // When migrating across engine we always add both of the token indexes.
            return 2 - statistics.lookupIndexes();
        }

        return statistics.kernelVersion().isLessThan(KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED)
                ? 1
                : 0;
    }

    protected void verifyStoreFormat(GraphDatabaseService db, StoreVersionIdentifier expectedFormat)
            throws IOException {
        GraphDatabaseAPI database = (GraphDatabaseAPI) db;
        DependencyResolver dependencyResolver = database.getDependencyResolver();
        PageCache pageCache = dependencyResolver.resolveDependency(PageCache.class);
        StorageEngineFactory storageEngineFactory = dependencyResolver.resolveDependency(StorageEngineFactory.class);
        var storeId = storageEngineFactory.retrieveStoreId(
                directory.getFileSystem(), database.databaseLayout(), pageCache, CursorContext.NULL_CONTEXT);
        assertEquals(expectedFormat.getFormatName(), storeId.getFormatName());
        assertEquals(expectedFormat.getMajorVersion(), storeId.getMajorVersion());
        assertEquals(expectedFormat.getMinorVersion(), storeId.getMinorVersion());
    }

    protected static void verifyTokenIndexes(GraphDatabaseService db) {
        // Make sure that we have at least the node token index and that all our token indexes have real ids now.
        List<IndexDefinition> tokenIndexes;
        try (Transaction tx = db.beginTx()) {
            tokenIndexes = StreamSupport.stream(tx.schema().getIndexes().spliterator(), false)
                    .filter(i -> i.getIndexType() == LOOKUP)
                    .toList();
        }

        int size = tokenIndexes.size();
        assertThat(size).isGreaterThan(0).isLessThanOrEqualTo(2);

        if (size == 1) {
            IndexDescriptor descriptor = ((IndexDefinitionImpl) tokenIndexes.get(0)).getIndexReference();
            assertThat(descriptor.getId()).isGreaterThanOrEqualTo(0);
            assertThat(descriptor.schema().entityType()).isEqualTo(EntityType.NODE);
        } else if (size == 2) {
            IndexDescriptor descriptor = ((IndexDefinitionImpl) tokenIndexes.get(0)).getIndexReference();
            IndexDescriptor descriptor2 = ((IndexDefinitionImpl) tokenIndexes.get(1)).getIndexReference();
            assertThat(descriptor.getId()).isGreaterThanOrEqualTo(0);
            assertThat(descriptor2.getId()).isGreaterThanOrEqualTo(0);
            assertThat(descriptor.schema().entityType())
                    .isNotEqualTo(descriptor2.schema().entityType());
        }
    }

    protected static void verifyKernelVersion(GraphDatabaseService db) {
        final var database = (GraphDatabaseAPI) db;
        final var kernelVersionProvider =
                database.getDependencyResolver().resolveDependency(KernelVersionProvider.class);
        assertThat(kernelVersionProvider.kernelVersion()).isEqualTo(LatestVersions.LATEST_KERNEL_VERSION);
    }

    protected void verifyRemovedIndexProviders(GraphDatabaseService db) {
        // Assert that the old index provider directories are deleted
        var fs = directory.getFileSystem();
        var databaseLayout = ((GraphDatabaseAPI) db).databaseLayout();
        var nativeBtreeDir = getIndexProviderDirectoryStructure(databaseLayout, SchemaRule44.NATIVE_BTREE_10)
                .rootDirectory();
        var luceneNativeDir = getIndexProviderDirectoryStructure(databaseLayout, SchemaRule44.LUCENE_NATIVE_30);
        assertThat(fs.fileExists(nativeBtreeDir))
                .as("index directory should have been removed during migration")
                .isFalse();
        assertThat(fs.fileExists(luceneNativeDir.rootDirectory()))
                .as("index directory should have been removed during migration")
                .isFalse();
    }

    private static IndexDirectoryStructure getIndexProviderDirectoryStructure(
            DatabaseLayout databaseLayout, IndexProviderDescriptor indexProviderDescriptor) {
        return IndexDirectoryStructure.directoriesByProvider(databaseLayout.databaseDirectory())
                .forProvider(indexProviderDescriptor);
    }

    protected static void verifyFulltextIndexes(GraphDatabaseService db, KernelVersion version) {
        if (version.isLessThan(KernelVersion.V5_0)) {
            // when doing 4.x -> 5.0 upgrade full text indexes in zipped stores become inconsistent due to mix of string
            // and string arrays on properties
            // dropping them to pass consistency check
            try (Transaction tx = db.beginTx()) {
                Iterables.stream(tx.schema().getIndexes())
                        .filter(i -> i.getIndexType() == FULLTEXT)
                        .forEach(IndexDefinition::drop);
                tx.commit();
            }
        }
    }

    private static void verifyInitialIndexState(InitialIndexStateMonitor initialIndexStateMonitor) {
        assertThat(initialIndexStateMonitor.allIndexStates).isNotEmpty();
        for (Map.Entry<IndexDescriptor, InternalIndexState> internalIndexStateEntry :
                initialIndexStateMonitor.allIndexStates.entrySet()) {
            assertThat(internalIndexStateEntry.getKey().getIndexType())
                    .isIn(org.neo4j.internal.schema.IndexType.LOOKUP, RANGE);
            assertThat(internalIndexStateEntry.getValue())
                    .withFailMessage(internalIndexStateEntry.getKey() + " was not ONLINE as expected: "
                            + internalIndexStateEntry.getValue())
                    .isEqualTo(ONLINE);
        }
    }

    private static void verifyGraphComponents(GraphDatabaseService system) {
        var systemGraphComponents =
                ((GraphDatabaseAPI) system).getDependencyResolver().resolveDependency(SystemGraphComponents.class);
        try (Transaction tx = system.beginTx()) {
            systemGraphComponents.forEach(component -> assertCurrent(tx, component));
            tx.commit();
        }
    }

    private static void assertCurrent(Transaction tx, SystemGraphComponent component) {
        var status = component.detect(tx);
        assertThat(status)
                .withFailMessage(
                        "SystemGraphComponent " + component.componentName() + " was not upgraded, state=" + status)
                .isEqualTo(SystemGraphComponent.Status.CURRENT);
    }

    protected static void verifyHasUniqueConstraint(
            List<ConstraintDefinition> constraints, Label label, String... property) {
        assertThat(constraints).anySatisfy(constraintDefinition -> {
            assertThat(constraintDefinition.getConstraintType()).isEqualTo(ConstraintType.UNIQUENESS);
            assertThat(constraintDefinition.getLabel()).isEqualTo(label);
            assertThat(Iterables.asList(constraintDefinition.getPropertyKeys())).isEqualTo(Arrays.asList(property));
        });
    }

    protected static void verifyHasIndex(List<IndexDefinition> indexes, Label label, String... property) {
        assertThat(indexes).anySatisfy(indexDefinition -> {
            assertThat(indexDefinition.getIndexType()).isEqualTo(IndexType.RANGE);
            assertThat(indexDefinition.isNodeIndex()).isEqualTo(true);
            assertThat(Iterables.asList(indexDefinition.getLabels())).isEqualTo(List.of(label));
            assertThat(Iterables.asList(indexDefinition.getPropertyKeys())).isEqualTo(Arrays.asList(property));
        });
    }

    protected static void consistencyCheck(Path targetDirectory, String databaseName)
            throws ConsistencyCheckIncompleteException {
        consistencyCheck(targetDirectory, databaseName, false);
    }

    protected static void consistencyCheck(Path targetDirectory, String databaseName, boolean skipIndexes)
            throws ConsistencyCheckIncompleteException {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        ConsistencyFlags consistencyFlags = ConsistencyFlags.DEFAULT;
        if (skipIndexes) {
            consistencyFlags = consistencyFlags.withoutCheckIndexes();
        }
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .set(GraphDatabaseSettings.logs_directory, targetDirectory.resolve("inconsistency-reports"))
                .build();
        ConsistencyCheckService.Result consistencyCheckResult = new ConsistencyCheckService(
                        RecordDatabaseLayout.of(Neo4jLayout.of(targetDirectory), databaseName))
                .with(config)
                .with(consistencyFlags)
                .with(logProvider)
                .runFullConsistencyCheck();
        assertThat(consistencyCheckResult.isSuccessful())
                .as(logProvider.serialize())
                .isTrue();
    }

    public record SystemDbMigration(ZippedStore zippedStore, boolean hasRelationshipTypeIndex) {}
}
