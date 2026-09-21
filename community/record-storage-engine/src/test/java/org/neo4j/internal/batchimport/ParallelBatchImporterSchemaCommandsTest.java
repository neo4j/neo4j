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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.input.EntityListInput;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.wal.EmptyLogTailMetadata;

@PageCacheExtension
@Neo4jLayoutExtension
class ParallelBatchImporterSchemaCommandsTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private final Config config = Config.defaults();
    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    void start() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void stop() throws Exception {
        jobScheduler.close();
    }

    @Test
    void shouldImportDataAndSchemaCommands() throws IOException {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false),
                new IndexCommand.Create.NodeRange("user_other", "User", List.of("other"), false));

        importer().doImport(input(commands, uniqueUsers(3)));

        var rules = loadSchemaRules();
        var constraints = rules.stream()
                .filter(ConstraintDescriptor.class::isInstance)
                .map(ConstraintDescriptor.class::cast)
                .toList();
        var indexes = rules.stream()
                .filter(IndexDescriptor.class::isInstance)
                .map(IndexDescriptor.class::cast)
                .toList();
        assertThat(constraints).hasSize(1);
        assertThat(constraints.get(0).getName()).isEqualTo("user_name");
        assertThat(indexes).extracting(IndexDescriptor::getName).contains("user_name", "user_other");
        var backingIndex = indexes.stream()
                .filter(index -> index.getName().equals("user_name"))
                .findFirst()
                .orElseThrow();
        assertThat(backingIndex.getOwningConstraintId())
                .hasValue(constraints.get(0).getId());
        assertThat(constraints.get(0).asIndexBackedConstraint().ownedIndexId()).isEqualTo(backingIndex.getId());
        assertNoTempSchemaIndexesRemain();
    }

    @Test
    void shouldFailImportOnUniquenessViolation() {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false));
        var nodes = new InputEntity[] {user(0, "same"), user(1, "same")};

        assertThatThrownBy(() -> importer().doImport(input(commands, nodes)))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("would have violated constraint");
        assertNoTempSchemaIndexesRemain();
    }

    private void assertNoTempSchemaIndexesRemain() {
        assertThat(fs.fileExists(databaseLayout
                        .databaseDirectory()
                        .resolve(DefaultImporterSchemaPopulation.TEMP_INDEXES_DIRECTORY)))
                .isFalse();
    }

    @Test
    void shouldRejectUnsupportedSchemaCommands() {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.RelationshipUniqueness("ru", "T", List.of("p"), false));

        assertThatThrownBy(() -> importer().doImport(input(commands, uniqueUsers(1))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("RelationshipUniqueness");
    }

    @Test
    void shouldImportWithoutSchemaCommandsAsBefore() {
        assertThatCode(() -> importer().doImport(input(List.of(), uniqueUsers(2))))
                .doesNotThrowAnyException();
    }

    private BatchImporter importer() {
        return new ParallelBatchImporter(
                databaseLayout,
                fs,
                PageCacheTracer.NULL,
                Configuration.DEFAULT,
                NullLogService.getInstance(),
                ExecutionMonitor.INVISIBLE,
                DefaultAdditionalIds.EMPTY,
                new EmptyLogTailMetadata(config),
                config,
                Monitor.NO_MONITOR,
                jobScheduler,
                Collector.STRICT,
                LogFilesInitializer.NULL,
                IndexImporterFactory.EMPTY,
                INSTANCE,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                this::indexProvidersAccess,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
    }

    private IndexProvidersAccess indexProvidersAccess() {
        return new DefaultIndexProvidersAccess(
                new RecordStorageEngineFactory(),
                fs,
                config,
                jobScheduler,
                NullLogService.getInstance(),
                PageCacheTracer.NULL,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
    }

    private List<SchemaRule> loadSchemaRules() {
        return new RecordStorageEngineFactory()
                .loadSchemaRules(
                        fs,
                        pageCache,
                        PageCacheTracer.NULL,
                        config,
                        databaseLayout,
                        true,
                        Function.identity(),
                        new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                        INSTANCE);
    }

    private static final Groups GROUPS = new Groups();
    private static final org.neo4j.batchimport.api.input.Group GLOBAL = GROUPS.getOrCreate(null);

    private static InputEntity[] uniqueUsers(int count) {
        var users = new InputEntity[count];
        for (int i = 0; i < count; i++) {
            users[i] = user(i, "user-" + i);
        }
        return users;
    }

    private static InputEntity user(long id, String name) {
        var user = new InputEntity();
        user.id(id, GLOBAL);
        user.labels(new String[] {"User"});
        user.property("name", name, false);
        return user;
    }

    private static Input input(List<SchemaCommand> commands, InputEntity[] nodes) {
        return new EntityListInput(GROUPS, List.of(nodes), List.of(), ResolvedSchemaCommands.of(commands));
    }
}
