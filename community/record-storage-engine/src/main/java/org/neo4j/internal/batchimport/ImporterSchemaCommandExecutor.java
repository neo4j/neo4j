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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.UnaryOperator;
import org.neo4j.exceptions.KernelException;
import org.neo4j.importer.SchemaCommandSource;
import org.neo4j.importer.SchemaCommandSource.DeferredSchemaCommands;
import org.neo4j.importer.SchemaCommandSource.ResolvedSchemaCommands;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

/**
 * Applies {@link SchemaCommand}s while batch importing into the record format, the record analog of the
 * block importer's schema manager. Supports only the subset needed to seed stores offline: node uniqueness
 * constraints and node RANGE indexes; everything else is rejected by {@link #validateSupported}.
 * <p>
 * The importer has no edition context, so the community-edition rule that only UNIQUE constraints may be
 * created (see {@code SchemaCommandsBuilder}) is not enforced here — the supported subset happens to satisfy
 * it. Widening the subset beyond UNIQUE constraints requires adding edition gating at the call sites.
 */
public class ImporterSchemaCommandExecutor {
    private static final String SCHEMA_IMPORT_TAG = "schemaCommandImport";

    private final BatchingNeoStores neoStores;
    private final List<SchemaCommand> commands;
    private final UnaryOperator<IndexDescriptor> indexConfigurationCompleter;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final List<IndexDescriptor> indexes = new ArrayList<>();
    private final List<ConstraintDescriptor> constraints = new ArrayList<>();

    public ImporterSchemaCommandExecutor(
            BatchingNeoStores neoStores,
            List<SchemaCommand> commands,
            UnaryOperator<IndexDescriptor> indexConfigurationCompleter,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.neoStores = neoStores;
        this.commands = commands;
        this.indexConfigurationCompleter = indexConfigurationCompleter;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
    }

    public static List<SchemaCommand> validateSupported(SchemaCommandSource source) {
        return switch (source) {
            case DeferredSchemaCommands ignored ->
                throw new UnsupportedOperationException(
                        "Importing into the record storage format requires the schema commands to be resolved before "
                                + "the import; unresolved commands, such as those read from a schema file by "
                                + "neo4j-admin, are not supported, so create those indexes or constraints on the "
                                + "database after the import completes");
            case ResolvedSchemaCommands resolved -> {
                var names = new HashSet<String>();
                var schemas = new HashMap<NodeSchema, SchemaCommand>();
                for (SchemaCommand command : resolved.commands()) {
                    if (!(command instanceof IndexCommand.Create.NodeRange
                            || command instanceof ConstraintCommand.Create.NodeUniqueness)) {
                        throw new UnsupportedOperationException(
                                "Record format batch import does not support schema command %s; only node uniqueness constraints and node range indexes are supported"
                                        .formatted(describe(command)));
                    }
                    if (command.name() != null && !command.name().isEmpty() && !names.add(command.name())) {
                        throw new IllegalArgumentException(
                                "Multiple schema commands named '%s'".formatted(command.name()));
                    }
                    // every supported command creates exactly one node RANGE index - directly, or as the backing
                    // index of a uniqueness constraint - so two commands on the same schema would write duplicate
                    // index rules, which the consistency checker reports as duplicateRuleContent
                    var schema = nodeSchema(command);
                    var clashing = schemas.putIfAbsent(schema, command);
                    if (clashing != null) {
                        throw new IllegalArgumentException("Schema commands %s and %s would both create an index on %s"
                                .formatted(describe(clashing), describe(command), schema.describe()));
                    }
                }
                yield resolved.commands();
            }
        };
    }

    private static NodeSchema nodeSchema(SchemaCommand command) {
        return switch (command) {
            case IndexCommand.Create.NodeRange index -> new NodeSchema(index.label(), index.properties());
            case ConstraintCommand.Create.NodeUniqueness constraint ->
                new NodeSchema(constraint.label(), constraint.properties());
            default ->
                throw new UnsupportedOperationException(
                        "Record format batch import does not support schema command %s".formatted(describe(command)));
        };
    }

    /** Composite indexes are order-sensitive, so {@code properties} is compared as an ordered list. */
    private record NodeSchema(String label, List<String> properties) {
        String describe() {
            return "(:%s {%s})".formatted(label, String.join(", ", properties));
        }
    }

    private static String describe(SchemaCommand command) {
        var description = new StringBuilder(command.getClass().getSimpleName());
        for (Class<?> enclosing = command.getClass().getEnclosingClass();
                enclosing != null && enclosing != SchemaCommand.class;
                enclosing = enclosing.getEnclosingClass()) {
            description.insert(0, enclosing.getSimpleName() + ".");
        }
        if (command.name() != null && !command.name().isEmpty()) {
            description.append(" '").append(command.name()).append("'");
        }
        return description.toString();
    }

    public void createSchemaRules() {
        var stores = neoStores.getNeoStores();
        var schemaStore = stores.getSchemaStore();
        var tokenHolders = neoStores.getTokenHolders();
        var allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(stores);
        var schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess(schemaStore, tokenHolders);
        SchemaCommand current = null;
        try (var cursorContext = contextFactory.create(SCHEMA_IMPORT_TAG);
                var storeCursors = new CachedStoreCursors(stores, cursorContext)) {
            createTokens(tokenHolders);
            for (SchemaCommand command : commands) {
                current = command;
                switch (command) {
                    case IndexCommand.Create.NodeRange index ->
                        createIndex(
                                index,
                                tokenHolders,
                                schemaStore.getIdGenerator(),
                                schemaRuleAccess,
                                allocatorProvider,
                                cursorContext,
                                storeCursors);
                    case ConstraintCommand.Create.NodeUniqueness constraint ->
                        createConstraint(
                                constraint,
                                tokenHolders,
                                schemaStore.getIdGenerator(),
                                schemaRuleAccess,
                                allocatorProvider,
                                cursorContext,
                                storeCursors);
                    default ->
                        throw new UnsupportedOperationException(
                                "Record format batch import does not support schema command %s"
                                        .formatted(describe(command)));
                }
            }
        } catch (KernelException e) {
            throw new RuntimeException(
                    current == null
                            ? "Error creating tokens for import schema commands"
                            : "Error creating schema rule for import: %s".formatted(describe(current)),
                    e);
        }
    }

    public List<IndexDescriptor> indexes() {
        return List.copyOf(indexes);
    }

    public List<ConstraintDescriptor> constraints() {
        return List.copyOf(constraints);
    }

    private void createTokens(TokenHolders tokenHolders) throws KernelException {
        for (SchemaCommand command : commands) {
            switch (command) {
                case IndexCommand.Create.NodeRange index -> {
                    tokenHolders.labelTokens().getOrCreateId(index.label());
                    getOrCreateIds(tokenHolders.propertyKeyTokens(), index.properties());
                }
                case ConstraintCommand.Create.NodeUniqueness constraint -> {
                    tokenHolders.labelTokens().getOrCreateId(constraint.label());
                    getOrCreateIds(tokenHolders.propertyKeyTokens(), constraint.properties());
                }
                default -> {}
            }
        }
    }

    private static void getOrCreateIds(TokenHolder tokenHolder, List<String> names) throws KernelException {
        for (String name : names) {
            tokenHolder.getOrCreateId(name);
        }
    }

    private void createIndex(
            IndexCommand.Create index,
            TokenHolders tokenHolders,
            IdGenerator idGenerator,
            SchemaRuleAccess schemaRuleAccess,
            DynamicAllocatorProvider allocatorProvider,
            CursorContext cursorContext,
            StoreCursors storeCursors)
            throws KernelException {
        IndexDescriptor descriptor = indexConfigurationCompleter.apply(
                index.toPrototype(tokenHolders).materialise(idGenerator.nextId(cursorContext)));
        schemaRuleAccess.writeSchemaRule(
                descriptor, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, memoryTracker, storeCursors);
        indexes.add(descriptor);
    }

    private void createConstraint(
            ConstraintCommand.Create constraint,
            TokenHolders tokenHolders,
            IdGenerator idGenerator,
            SchemaRuleAccess schemaRuleAccess,
            DynamicAllocatorProvider allocatorProvider,
            CursorContext cursorContext,
            StoreCursors storeCursors)
            throws KernelException {
        var prototype = constraint.toPrototype(tokenHolders);
        long indexId = idGenerator.nextId(cursorContext);
        long constraintId = idGenerator.nextId(cursorContext);
        ConstraintDescriptor constraintRule =
                prototype.descriptor().withOwnedIndexId(indexId).withId(constraintId);
        IndexDescriptor backingIndex = indexConfigurationCompleter
                .apply(prototype.backingIndex().materialise(indexId))
                .withOwningConstraintId(constraintId);
        schemaRuleAccess.writeSchemaRule(
                backingIndex, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, memoryTracker, storeCursors);
        schemaRuleAccess.writeSchemaRule(
                constraintRule, IdUpdateListener.DIRECT, allocatorProvider, cursorContext, memoryTracker, storeCursors);
        indexes.add(backingIndex);
        constraints.add(constraintRule);
    }
}
