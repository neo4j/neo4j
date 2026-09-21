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
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.configuration.Config;
import org.neo4j.importer.SchemaCommandSource.DeferredSchemaCommands;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.token.api.TokenConstants;

@PageCacheExtension
@Neo4jLayoutExtension
class ImporterSchemaCommandExecutorTest {
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
                NULL,
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
    void shouldWriteCrossReferencedRulesForNodeUniquenessConstraint() {
        var command = new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false);

        execute(command);

        var indexes = readIndexes();
        var constraints = readConstraints();
        assertThat(indexes).hasSize(1);
        assertThat(constraints).hasSize(1);
        IndexDescriptor index = indexes.get(0);
        ConstraintDescriptor constraint = constraints.get(0);
        assertThat(index.getName()).isEqualTo("user_name");
        assertThat(index.isUnique()).isTrue();
        assertThat(index.getIndexType()).isEqualTo(IndexType.RANGE);
        assertThat(index.getOwningConstraintId()).hasValue(constraint.getId());
        assertThat(constraint.getName()).isEqualTo("user_name");
        assertThat(constraint.asIndexBackedConstraint().ownedIndexId()).isEqualTo(index.getId());
    }

    @Test
    void shouldWritePlainNodeRangeIndex() {
        var command = new IndexCommand.Create.NodeRange("qualifier_index", "Qualifier", List.of("value"), false);

        execute(command);

        var indexes = readIndexes();
        assertThat(readConstraints()).isEmpty();
        assertThat(indexes).hasSize(1);
        IndexDescriptor index = indexes.get(0);
        assertThat(index.getName()).isEqualTo("qualifier_index");
        assertThat(index.isUnique()).isFalse();
        assertThat(index.getIndexType()).isEqualTo(IndexType.RANGE);
        assertThat(index.getOwningConstraintId()).isEmpty();
    }

    @Test
    void shouldCreateTokensReferencedOnlyBySchema() {
        execute(
                new ConstraintCommand.Create.NodeUniqueness("c", "SchemaOnlyLabel", List.of("schemaOnlyProp"), false),
                new IndexCommand.Create.NodeRange("i", "OtherLabel", List.of("p1", "p2"), false));

        var tokenHolders = stores.getTokenHolders();
        int label = tokenHolders.labelTokens().getIdByName("SchemaOnlyLabel");
        int property = tokenHolders.propertyKeyTokens().getIdByName("schemaOnlyProp");
        assertThat(label).isNotEqualTo(TokenConstants.NO_TOKEN);
        assertThat(property).isNotEqualTo(TokenConstants.NO_TOKEN);
        assertThat(tokenHolders.labelTokens().getIdByName("OtherLabel")).isNotEqualTo(TokenConstants.NO_TOKEN);
        assertThat(tokenHolders.propertyKeyTokens().getIdByName("p2")).isNotEqualTo(TokenConstants.NO_TOKEN);

        var constraint = readConstraints().get(0);
        assertThat(constraint.schema().getEntityTokenIds()).containsExactly(label);
        assertThat(constraint.schema().getPropertyIds()).containsExactly(property);
    }

    @Test
    void shouldGenerateKernelDefaultNamesForUnnamedCommands() {
        execute(
                new ConstraintCommand.Create.NodeUniqueness(null, "User", List.of("name"), false),
                new IndexCommand.Create.NodeRange(null, "Qualifier", List.of("value"), false));

        var tokenHolders = stores.getTokenHolders();
        var expectedConstraintName = new ConstraintCommand.Create.NodeUniqueness(null, "User", List.of("name"), false)
                .toPrototype(tokenHolders)
                .descriptor()
                .getName();
        var expectedIndexName = new IndexCommand.Create.NodeRange(null, "Qualifier", List.of("value"), false)
                .toPrototype(tokenHolders)
                .getName()
                .orElseThrow();

        assertThat(readConstraints().get(0).getName())
                .isEqualTo(expectedConstraintName)
                .startsWith("constraint_");
        var plainIndex = readIndexes().stream()
                .filter(index -> !index.isUnique())
                .findFirst()
                .orElseThrow();
        assertThat(plainIndex.getName()).isEqualTo(expectedIndexName).startsWith("index_");
    }

    @Test
    void shouldRejectUnsupportedCommands() {
        List<SchemaCommand> unsupported = List.of(
                new IndexCommand.Create.NodeText("t", "Label", "prop", false),
                new IndexCommand.Create.RelationshipRange("r", "TYPE", List.of("prop"), false),
                new ConstraintCommand.Create.RelationshipUniqueness("ru", "TYPE", List.of("prop"), false),
                new ConstraintCommand.Create.NodeExistence("ne", "Label", "prop", false, false),
                new IndexCommand.Drop("someIndex", false),
                new ConstraintCommand.Drop("someConstraint", false));

        for (SchemaCommand command : unsupported) {
            assertThatThrownBy(
                            () -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(command)))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining(command.getClass().getSimpleName())
                    .hasMessageContaining("'" + command.name() + "'");
        }
    }

    @Test
    void shouldNotPrintNullNameForUnnamedUnsupportedCommands() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(
                        ResolvedSchemaCommands.of(new IndexCommand.Create.NodeText(null, "Label", "prop", false))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("NodeText")
                .hasMessageNotContaining("'null'");
    }

    @Test
    void shouldRejectDuplicateCommandNames() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(
                        new ConstraintCommand.Create.NodeUniqueness("dup", "A", List.of("p"), false),
                        new IndexCommand.Create.NodeRange("dup", "B", List.of("q"), false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dup");
    }

    @Test
    void shouldRejectDifferentlyNamedIndexesOnTheSameSchema() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(
                        new IndexCommand.Create.NodeRange("a", "User", List.of("name"), false),
                        new IndexCommand.Create.NodeRange("b", "User", List.of("name"), false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'a'")
                .hasMessageContaining("'b'")
                .hasMessageContaining("(:User {name})");
    }

    @Test
    void shouldRejectUnnamedIndexesOnTheSameSchema() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(
                        new IndexCommand.Create.NodeRange(null, "User", List.of("name"), false),
                        new IndexCommand.Create.NodeRange(null, "User", List.of("name"), false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("(:User {name})");
    }

    @Test
    void shouldRejectIndexClashingWithConstraintBackingIndex() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(
                        new ConstraintCommand.Create.NodeUniqueness("user_name", "User", List.of("name"), false),
                        new IndexCommand.Create.NodeRange("user_name_index", "User", List.of("name"), false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'user_name'")
                .hasMessageContaining("'user_name_index'");
    }

    @Test
    void shouldRejectDuplicateConstraintsOnTheSameSchema() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(
                        new ConstraintCommand.Create.NodeUniqueness("first", "User", List.of("name"), false),
                        new ConstraintCommand.Create.NodeUniqueness("second", "User", List.of("name"), false))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'first'")
                .hasMessageContaining("'second'");
    }

    @Test
    void shouldAcceptCompositeIndexesDifferingOnlyInPropertyOrder() {
        var first = new IndexCommand.Create.NodeRange("first", "User", List.of("first", "last"), false);
        var second = new IndexCommand.Create.NodeRange("second", "User", List.of("last", "first"), false);
        assertThat(ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(first, second)))
                .containsExactly(first, second);
    }

    @Test
    void shouldAcceptSamePropertiesOnDifferentLabels() {
        var first = new IndexCommand.Create.NodeRange("first", "User", List.of("name"), false);
        var second = new ConstraintCommand.Create.NodeUniqueness("second", "Group", List.of("name"), false);
        assertThat(ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(first, second)))
                .containsExactly(first, second);
    }

    @Test
    void shouldRejectDeferredSchemaCommandsWithOperatorFacingMessage() {
        assertThatThrownBy(() -> ImporterSchemaCommandExecutor.validateSupported(
                        new DeferredSchemaCommands((adaptor, tokenHolders) -> List.of())))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("requires the schema commands to be resolved before the import")
                .hasMessageContaining("create those indexes or constraints on the database after the import completes")
                .hasMessageNotContaining("DeferredSchemaCommands");
    }

    @Test
    void shouldAcceptEmptyResolvedCommands() {
        assertThat(ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of()))
                .isEmpty();
    }

    private void execute(SchemaCommand... commands) {
        var executor = new ImporterSchemaCommandExecutor(
                stores,
                ImporterSchemaCommandExecutor.validateSupported(ResolvedSchemaCommands.of(commands)),
                UnaryOperator.identity(),
                NULL_CONTEXT_FACTORY,
                INSTANCE);
        executor.createSchemaRules();
        assertThat(executor.indexes()).hasSameSizeAs(readIndexes());
    }

    private List<IndexDescriptor> readIndexes() {
        var schemaRuleAccess =
                SchemaRuleAccess.getSchemaRuleAccess(stores.getNeoStores().getSchemaStore(), stores.getTokenHolders());
        try (var storeCursors = new CachedStoreCursors(stores.getNeoStores(), CursorContext.NULL_CONTEXT)) {
            var result = new ArrayList<IndexDescriptor>();
            schemaRuleAccess.indexesGetAll(storeCursors, INSTANCE).forEachRemaining(result::add);
            return result;
        }
    }

    private List<ConstraintDescriptor> readConstraints() {
        var schemaRuleAccess =
                SchemaRuleAccess.getSchemaRuleAccess(stores.getNeoStores().getSchemaStore(), stores.getTokenHolders());
        try (var storeCursors = new CachedStoreCursors(stores.getNeoStores(), CursorContext.NULL_CONTEXT)) {
            var result = new ArrayList<ConstraintDescriptor>();
            schemaRuleAccess
                    .constraintsGetAllIgnoreMalformed(storeCursors, INSTANCE)
                    .forEachRemaining(result::add);
            return result;
        }
    }
}
