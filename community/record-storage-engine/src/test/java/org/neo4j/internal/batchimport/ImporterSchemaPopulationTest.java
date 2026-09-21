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
package org.neo4j.internal.batchimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.input.ApplicationMode;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
class ImporterSchemaPopulationTest {
    private static final SchemaMonitor.ExistingPropertyKeysLookup NO_EXISTING_PROPERTIES =
            (entityId, keysToLookup) -> IntSets.immutable.empty();
    private static final SchemaMonitor.UniquenessIndexUpdatesListener NO_UNIQUENESS_LISTENER =
            new SchemaMonitor.UniquenessIndexUpdatesListener() {
                @Override
                public boolean shouldApply(org.neo4j.storageengine.api.EagerValueIndexEntryUpdate update) {
                    return true;
                }

                @Override
                public void updates(List<org.neo4j.storageengine.api.EagerValueIndexEntryUpdate> updates) {}

                @Override
                public void endEntity() {}
            };

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RecordDatabaseLayout layout;

    private ThreadPoolJobScheduler jobScheduler;
    private BatchingNeoStores stores;

    @BeforeEach
    void start() throws IOException {
        jobScheduler = new ThreadPoolJobScheduler();
        stores = BatchingNeoStores.batchingNeoStoresWithExternalPageCache(
                fs,
                pageCache,
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY,
                layout,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                Config.defaults(),
                INSTANCE);
        stores.createNew();
    }

    @AfterEach
    void stop() throws IOException {
        stores.close();
        jobScheduler.close();
    }

    @Test
    void shouldPopulateBackingIndexesFromEntityUpdates() throws IOException {
        List<IndexDescriptor> indexes;
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false),
                new IndexCommand.Create.NodeRange("user_age", "User", List.of("age"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                feedNodes(nodeMonitors, 100, i -> "user-" + i);
                validateAndWriteToTarget(nodeMonitors);
            }
            indexes = indexesOf(population);
            population.completeAfterImport();
        }

        assertThat(indexes).hasSize(2);
        assertOnline(indexes);
    }

    @Test
    void shouldPopulateOneIndexAndBackfillSiblingWithoutData() throws IOException {
        List<IndexDescriptor> indexes;
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false),
                new IndexCommand.Create.NodeRange("user_other", "User", List.of("other"), false))) {
            try (var nodeMonitors = population.nodeMonitors();
                    var relationshipMonitors = population.relationshipMonitors()) {
                feedNodes(nodeMonitors, 10, i -> "user-" + i);
                validateAndWriteToTarget(nodeMonitors);
                validateAndWriteToTarget(relationshipMonitors);
            }
            indexes = indexesOf(population);
            population.completeAfterImport();
        }

        assertThat(indexes).hasSize(2);
        assertOnline(indexes);
    }

    @Test
    void shouldFailOnDuplicateUniqueValues() throws IOException {
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                feedNodes(nodeMonitors, 2, i -> "same-value");
                assertThatThrownBy(() -> validateAndWriteToTarget(nodeMonitors))
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasStackTraceContaining("would have violated constraint");
            }
        }
    }

    @Test
    void shouldBackfillUntouchedIndexesAsOnlineAndEmpty() throws IOException {
        List<IndexDescriptor> indexes;
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("authrule_1", "AuthRule", List.of("name"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                validateAndWriteToTarget(nodeMonitors);
            }
            indexes = indexesOf(population);
            population.completeAfterImport();
        }

        assertThat(indexes).hasSize(1);
        assertOnline(indexes);
    }

    @Test
    void shouldBackfillUntouchedIndexWhoseTargetDirectoryAlreadyExists() throws IOException {
        List<IndexDescriptor> indexes;
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("authrule_1", "AuthRule", List.of("name"), false))) {
            indexes = indexesOf(population);
            // a leftover directory must not be mistaken for a populated index
            createTargetIndexDirectories(indexes);
            try (var nodeMonitors = population.nodeMonitors()) {
                validateAndWriteToTarget(nodeMonitors);
            }
            population.completeAfterImport();
        }

        assertThat(indexes).hasSize(1);
        assertOnline(indexes);
    }

    private void createTargetIndexDirectories(List<IndexDescriptor> indexes) throws IOException {
        try (var providersAccess = indexProvidersAccess()) {
            var providers = providersAccess.access(pageCache, layout, writableChecker(), stores.getTokenHolders());
            for (IndexDescriptor index : indexes) {
                var provider = providers.lookup(index.getIndexProvider());
                fs.mkdirs(provider.directoryStructure().directoryForIndex(index.getId()));
            }
        }
    }

    @Test
    void shouldCloseCleanlyWithoutComplete() throws IOException {
        var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false));
        population.nodeMonitors().close();
        population.close();
        assertThat(fs.fileExists(tempIndexesDirectory())).isFalse();
    }

    @Test
    void shouldDeleteTempIndexesOnCloseAfterSuccessfulImport() throws IOException {
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                feedNodes(nodeMonitors, 10, i -> "user-" + i);
                assertThat(fs.fileExists(tempIndexesDirectory())).isTrue();
                validateAndWriteToTarget(nodeMonitors);
            }
            population.completeAfterImport();
        }
        assertThat(fs.fileExists(tempIndexesDirectory())).isFalse();
    }

    @Test
    void shouldDeleteTempIndexesOnCloseAfterFailedValidation() throws IOException {
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                feedNodes(nodeMonitors, 2, i -> "same-value");
                assertThatThrownBy(() -> validateAndWriteToTarget(nodeMonitors))
                        .hasRootCauseInstanceOf(IllegalStateException.class);
            }
        }
        assertThat(fs.fileExists(tempIndexesDirectory())).isFalse();
    }

    @Test
    void shouldRejectMonitorsAfterComplete() throws IOException {
        try (var population = createPopulation(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false))) {
            try (var nodeMonitors = population.nodeMonitors()) {
                validateAndWriteToTarget(nodeMonitors);
            }
            population.completeAfterImport();
            assertThatThrownBy(population::nodeMonitors)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already completed");
        }
    }

    @Test
    void shouldReturnNoneForEmptyCommands() throws IOException {
        try (var population = createPopulation()) {
            assertThat(population).isSameAs(ImporterSchemaPopulation.NONE);
            assertThat(population.nodeMonitors()).isSameAs(SchemaMonitors.NO_SCHEMA);
            assertThat(population.relationshipMonitors()).isSameAs(SchemaMonitors.NO_SCHEMA);
            population.completeAfterImport();
        }
    }

    @Test
    void shouldCloseProvidersAndDeleteTempIndexesWhenCreateFails() {
        var providersClosed = new AtomicBoolean();
        Supplier<IndexProvidersAccess> trackingProviders = () -> {
            var delegate = indexProvidersAccess();
            return new IndexProvidersAccess() {
                @Override
                public IndexProviderMap access(
                        PageCache pageCache,
                        DatabaseLayout layout,
                        DatabaseReadOnlyChecker readOnlyChecker,
                        MemoryTracker memoryTracker) {
                    return delegate.access(pageCache, layout, readOnlyChecker, memoryTracker);
                }

                @Override
                public IndexProviderMap access(
                        PageCache pageCache,
                        DatabaseLayout layout,
                        DatabaseReadOnlyChecker readOnlyChecker,
                        TokenHolders tokenHolders) {
                    return delegate.access(pageCache, layout, readOnlyChecker, tokenHolders);
                }

                @Override
                public void close() throws IOException {
                    providersClosed.set(true);
                    delegate.close();
                }
            };
        };

        assertThatThrownBy(() -> ImporterSchemaPopulation.create(
                        List.of(new ConstraintCommand.Create.NodeKey("user_key", "User", List.of("name"), false)),
                        stores,
                        fs,
                        trackingProviders,
                        layout,
                        Configuration.DEFAULT,
                        Config.defaults(),
                        jobScheduler,
                        PageCacheTracer.NULL,
                        NULL_CONTEXT_FACTORY,
                        INSTANCE))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support schema command");

        assertThat(providersClosed).isTrue();
        assertThat(fs.fileExists(tempIndexesDirectory())).isFalse();
    }

    @Test
    void shouldFailDiagnosablyWhenSchemaCommandsUsedWithoutIndexProviders() {
        assertThatThrownBy(() -> ImporterSchemaPopulation.create(
                        ImporterSchemaCommandExecutor.validateSupported(
                                ResolvedSchemaCommands.of(new ConstraintCommand.Create.NodeUniqueness(
                                        "user_name", "User", List.of("name"), false))),
                        stores,
                        fs,
                        BatchImporterFactory.NO_INDEX_PROVIDERS,
                        layout,
                        Configuration.DEFAULT,
                        Config.defaults(),
                        jobScheduler,
                        PageCacheTracer.NULL,
                        NULL_CONTEXT_FACTORY,
                        INSTANCE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without index providers");
    }

    /**
     * Mirrors the end-of-stage sequence {@code ImportLogic.importNodes/importRelationships} run in production
     * after the data stages have fed the monitors.
     */
    private static void validateAndWriteToTarget(SchemaMonitors monitors) throws IOException {
        var affectedMonitors = (OtherAffectedSchemaMonitors) monitors;
        var violating = affectedMonitors.validate(Collector.STRICT, ProgressMonitorFactory.NONE);
        affectedMonitors.writeToTarget(null, violating, ProgressMonitorFactory.NONE);
    }

    private static List<IndexDescriptor> indexesOf(ImporterSchemaPopulation population) {
        return ((DefaultImporterSchemaPopulation) population).indexes();
    }

    private java.nio.file.Path tempIndexesDirectory() {
        return layout.databaseDirectory().resolve(DefaultImporterSchemaPopulation.TEMP_INDEXES_DIRECTORY);
    }

    private ImporterSchemaPopulation createPopulation(SchemaCommand... commands) throws IOException {
        return ImporterSchemaPopulation.create(
                ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(commands)),
                stores,
                fs,
                this::indexProvidersAccess,
                layout,
                Configuration.DEFAULT,
                Config.defaults(),
                jobScheduler,
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY,
                INSTANCE);
    }

    private IndexProvidersAccess indexProvidersAccess() {
        return new DefaultIndexProvidersAccess(
                new RecordStorageEngineFactory(),
                fs,
                Config.defaults(),
                jobScheduler,
                NullLogService.getInstance(),
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY);
    }

    private void feedNodes(SchemaMonitors schemaMonitors, int count, java.util.function.IntFunction<String> name) {
        var tokenHolders = stores.getTokenHolders();
        int label = tokenHolders.labelTokens().getIdByName("User");
        int nameKey = tokenHolders.propertyKeyTokens().getIdByName("name");
        int ageKey = tokenHolders.propertyKeyTokens().getIdByName("age");
        try (var monitor = schemaMonitors.newMonitor(0)) {
            for (int i = 0; i < count; i++) {
                List<StorageProperty> properties = ageKey != org.neo4j.token.api.TokenConstants.NO_TOKEN
                        ? List.of(
                                new PropertyKeyValue(nameKey, Values.of(name.apply(i))),
                                new PropertyKeyValue(ageKey, Values.of(i)))
                        : List.of(new PropertyKeyValue(nameKey, Values.of(name.apply(i))));
                var entity = new SchemaMonitor.Entity(
                        "input-" + i,
                        i,
                        properties,
                        List.of(),
                        null,
                        false,
                        IntSets.immutable.empty(),
                        IntSets.immutable.empty(),
                        IntSets.immutable.of(label),
                        IntSets.immutable.empty(),
                        ApplicationMode.CREATE,
                        "test",
                        i);
                boolean ok = monitor.handle(
                        entity,
                        NO_EXISTING_PROPERTIES,
                        (violating, description) -> fail("Unexpected violation for %s: %s", violating, description),
                        NO_UNIQUENESS_LISTENER);
                assertThat(ok).isTrue();
            }
        }
    }

    private void assertOnline(List<IndexDescriptor> indexes) throws IOException {
        try (var providersAccess = indexProvidersAccess()) {
            var providers = providersAccess.access(pageCache, layout, writableChecker(), stores.getTokenHolders());
            for (IndexDescriptor index : indexes) {
                var provider = providers.lookup(index.getIndexProvider());
                assertThat(provider.getInitialState(index, NULL_CONTEXT, stores.getOpenOptions()))
                        .as("initial state of %s", index.getName())
                        .isEqualTo(InternalIndexState.ONLINE);
            }
        }
    }

    private static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker writableChecker() {
        return org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable();
    }
}
