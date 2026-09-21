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

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.logging.LogAssertions.assertThatThrownBy;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.input.EntityListInput;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RequireAlignedFormat;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.wal.EmptyLogTailMetadata;
import org.neo4j.wal.files.TransactionLogInitializer;

@Neo4jLayoutExtension
@RequireAlignedFormat
public abstract class BatchImporterSchemaCommandsITBase {
    private static final Groups GROUPS = new Groups();
    private static final Group GLOBAL = GROUPS.getOrCreate(null);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private final Config dbConfig = Config.defaults();
    private ThreadPoolJobScheduler jobScheduler;

    @BeforeEach
    protected void start() {
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    protected void stop() throws Exception {
        jobScheduler.close();
    }

    protected abstract Class<? extends BatchImporterFactory> expectedFactoryClass();

    @Test
    protected void importerFactorySelectedForThisClasspath() {
        assertThat(BatchImporterFactory.withHighestPriority()).isInstanceOf(expectedFactoryClass());
    }

    @Test
    protected void shouldProduceConsistentStoreWithOnlineIndexesAndEnforcedConstraints() throws Exception {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false),
                new ConstraintCommand.Create.NodeUniqueness("user_first_last", "User", List.of("first", "last"), false),
                new IndexCommand.Create.NodeRange("user_age", "User", List.of("age"), false),
                new IndexCommand.Create.NodeRange("user_last_first", "User", List.of("last", "first"), false),
                new IndexCommand.Create.NodeRange("user_other", "User", List.of("other"), false));
        var nodes = users(10);
        var relationships = new InputEntity[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            relationships[i] = knows(i, (i + 1) % nodes.length);
        }

        importer().doImport(input(commands, nodes, relationships));

        var logProvider = new AssertableLogProvider();
        var managementService = new TestDatabaseManagementServiceBuilder(databaseLayout)
                .setInternalLogProvider(logProvider)
                .build();
        try {
            var db = managementService.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                assertThat(tx.schema().getConstraints())
                        .extracting(ConstraintDefinition::getName)
                        .containsExactlyInAnyOrder("user_name", "user_first_last");
                assertThat(tx.schema().getIndexes())
                        .extracting(IndexDefinition::getName)
                        .contains("user_name", "user_first_last", "user_age", "user_last_first", "user_other");
                assertThat(tx.schema().getIndexes())
                        .allSatisfy(index ->
                                assertThat(tx.schema().getIndexState(index)).isEqualTo(Schema.IndexState.ONLINE));
                assertThat(tx.getAllRelationships().stream()).hasSize(relationships.length);
                // two users share each age, so a seek only returns both if the plain index kept every entry
                try (var seek = tx.findNodes(Label.label("User"), "age", 1)) {
                    assertThat(seek.stream()).hasSize(2);
                }
            }
            // the uniqueness checks consult the backing indexes, so these only fail if those hold the imported entries
            assertThatThrownBy(() -> createUser(db, user -> user.setProperty("name", "user-1")))
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("already exists");
            assertThatThrownBy(() -> createUser(db, user -> {
                        user.setProperty("first", "first-1");
                        user.setProperty("last", "last-1");
                    }))
                    .isInstanceOf(ConstraintViolationException.class)
                    .hasMessageContaining("already exists");
            // the same properties in the opposite order are a different schema, so they do not clash
            createUser(db, user -> {
                user.setProperty("first", "last-1");
                user.setProperty("last", "first-1");
            });
            assertThat(logProvider.serialize().lines())
                    .as("no index repopulation on the imported database")
                    .noneMatch(line -> line.contains("Index population started")
                            && line.contains("[" + DEFAULT_DATABASE_NAME + "/"));
        } finally {
            managementService.shutdown();
        }

        assertConsistent();
    }

    @Test
    protected void shouldFailImportOnDuplicateValuesForUniquenessConstraint() {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false));
        var nodes = new InputEntity[] {user(0, "same"), user(1, "same")};

        assertThatThrownBy(() -> importer().doImport(input(commands, nodes)))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("would have violated constraint");
    }

    @Test
    protected void shouldRejectUnsupportedSchemaCommands() {
        var commands = List.<SchemaCommand>of(
                new ConstraintCommand.Create.RelationshipUniqueness("rel_unique", "T", List.of("p"), false));

        assertThatThrownBy(() -> importer().doImport(input(commands, uniqueUsers(1))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("RelationshipUniqueness");
    }

    private BatchImporter importer() {
        return BatchImporterFactory.withHighestPriority()
                .instantiate(
                        databaseLayout,
                        fs,
                        PageCacheTracer.NULL,
                        Configuration.DEFAULT,
                        NullLogService.getInstance(),
                        ExecutionMonitor.INVISIBLE,
                        DefaultAdditionalIds.EMPTY,
                        new EmptyLogTailMetadata(dbConfig),
                        dbConfig,
                        Monitor.NO_MONITOR,
                        jobScheduler,
                        Collector.STRICT,
                        TransactionLogInitializer.getLogFilesInitializer(),
                        new IndexImporterFactoryImpl(),
                        INSTANCE,
                        NULL_CONTEXT_FACTORY,
                        this::indexProvidersAccess,
                        DatabaseCreationOptions.EMPTY_CREATION_OPTIONS);
    }

    private IndexProvidersAccess indexProvidersAccess() {
        return new DefaultIndexProvidersAccess(
                new RecordStorageEngineFactory(),
                fs,
                dbConfig,
                jobScheduler,
                NullLogService.getInstance(),
                PageCacheTracer.NULL,
                NULL_CONTEXT_FACTORY);
    }

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

    /**
     * Carries a property for every supported schema shape except {@code other}, which stays without data.
     * {@code age} repeats across pairs of users, the rest are unique.
     */
    private static InputEntity[] users(int count) {
        var users = new InputEntity[count];
        for (int i = 0; i < count; i++) {
            var user = user(i, "user-" + i);
            user.property("first", "first-" + i, false);
            user.property("last", "last-" + i, false);
            user.property("age", i / 2, false);
            users[i] = user;
        }
        return users;
    }

    private static void createUser(GraphDatabaseService db, Consumer<Node> properties) {
        try (var tx = db.beginTx()) {
            properties.accept(tx.createNode(Label.label("User")));
            tx.commit();
        }
    }

    private static InputEntity knows(long startId, long endId) {
        var knows = new InputEntity();
        knows.startId(startId, GLOBAL);
        knows.endId(endId, GLOBAL);
        knows.type("KNOWS");
        return knows;
    }

    private void assertConsistent() throws Exception {
        var result = new ConsistencyCheckService(databaseLayout).runFullConsistencyCheck();
        assertThat(result.isSuccessful())
                .as("consistency check on the imported store")
                .isTrue();
    }

    private static Input input(List<SchemaCommand> commands, InputEntity[] nodes) {
        return input(commands, nodes, new InputEntity[0]);
    }

    private static Input input(List<SchemaCommand> commands, InputEntity[] nodes, InputEntity[] relationships) {
        return new EntityListInput(GROUPS, List.of(nodes), List.of(relationships), ResolvedSchemaCommands.of(commands));
    }
}
