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
package org.neo4j.kernel.impl.storemigration;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.RecordStorageMigrator.createTokenHolders;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.LegacyMetadataHandler;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.SchemaStoreMigration.SchemaStoreMigrator;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore44Reader;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;

public class SchemaStore44Migration {

    public static final IndexPrototype NLI_PROTOTYPE = IndexPrototype.forSchema(
                    SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR,
                    new IndexProviderDescriptor("token-lookup", "1.0"))
            .withIndexType(IndexType.LOOKUP)
            .withName("__org_neo4j_schema_index_label_scan_store_converted_to_token_index");

    /**
     * If a BTREE index has a replacement index - RANGE, TEXT or POINT index on same schema - the BTREE index will be removed.
     * If BTREE index doesn't have any replacement, an exception will be thrown (unless force=true in which case they
     * are replaced with RANGE equivalents).
     * If a constraint backed by a BTREE index has a replacement constraint - constraint of same type, on same schema,
     * backed by other index type than BTREE - the BTREE backed constraint will be removed.
     * If constraint backed by BTREE index doesn't have any replacement, an exception will be thrown (unless force=true
     * in which case they are replaced with RANGE equivalents).
     *
     * The SchemaStore (and naturally also the PropertyStore) will be updated non-transactionally.
     *
     * BTREE index type was deprecated in 4.4 and removed in 5.0.
     */
    public record SchemaStore44Migrator(
            boolean shouldCreateNewSchemaStore,
            boolean forceBtreeIndexesToRange,
            List<SchemaRule44.Index> nonReplacedIndexes,
            List<Pair<SchemaRule44.Constraint, SchemaRule44.Index>> nonReplacedConstraints,
            List<SchemaRule44> toDelete,
            List<SchemaRule> existingSchemaRulesToAdd,
            TokenHolders srcTokenHolders)
            implements SchemaStoreMigrator {

        @Override
        public void assertCanMigrate() throws IllegalStateException {
            if (!forceBtreeIndexesToRange && (!nonReplacedIndexes.isEmpty() || !nonReplacedConstraints.isEmpty())) {
                // Throw if non-replaced index exists
                var nonReplacedIndexString = new StringJoiner(", ", "[", "]");
                var nonReplacedConstraintsString = new StringJoiner(", ", "[", "]");
                nonReplacedIndexes.forEach(index -> nonReplacedIndexString.add(index.userDescription(srcTokenHolders)));
                nonReplacedConstraints.forEach(
                        pair -> nonReplacedConstraintsString.add(pair.first().userDescription(srcTokenHolders)));
                throw new IllegalStateException(
                        "Migration will remove all BTREE indexes and constraints backed by BTREE indexes. "
                                + "To guard against unintentionally removing indexes or constraints, "
                                + "it is recommended for all BTREE indexes or constraints backed by BTREE indexes to have a valid replacement. "
                                + "Indexes can be replaced by RANGE, TEXT or POINT index and constraints can be replaced by constraints backed by RANGE index. "
                                + "Please drop your indexes and constraints or create replacements and retry the migration. "
                                + "The indexes and constraints without replacement are: "
                                + nonReplacedIndexString + " and " + nonReplacedConstraintsString + ". "
                                + "Alternatively, you can use the option --force-btree-indexes-to-range to force all BTREE indexes or constraints backed by "
                                + "BTREE indexes to be replaced by RANGE equivalents. Be aware that RANGE indexes are not always the optimal replacement of BTREEs "
                                + "and performance may be affected while the new indexes are populated. See the Neo4j v5 migration guide online for more information.");
            }
        }

        @Override
        public void copyFilesInPreparationForMigration(
                FileSystemAbstraction fileSystem,
                RecordDatabaseLayout directoryLayout,
                RecordDatabaseLayout migrationLayout)
                throws IOException {
            List<DatabaseFile> databaseFiles;
            if (shouldCreateNewSchemaStore) {
                // In family migrations (or when migrating properties because of changed property record format)
                // a new schema store is created - old schema store should not be copied
                databaseFiles = asList(
                        RecordDatabaseFile.PROPERTY_STORE,
                        RecordDatabaseFile.PROPERTY_ARRAY_STORE,
                        RecordDatabaseFile.PROPERTY_STRING_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE);
            } else {
                databaseFiles = asList(
                        RecordDatabaseFile.SCHEMA_STORE,
                        RecordDatabaseFile.PROPERTY_STORE,
                        RecordDatabaseFile.PROPERTY_ARRAY_STORE,
                        RecordDatabaseFile.PROPERTY_STRING_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE);
            }

            fileOperation(
                    COPY,
                    fileSystem,
                    directoryLayout,
                    migrationLayout,
                    databaseFiles,
                    true,
                    true,
                    ExistingTargetStrategy.SKIP);
        }

        @Override
        public void migrate(SchemaRuleMigrationAccess dstAccess, TokenHolders dstTokensHolders) throws KernelException {
            boolean foundNliWithoutId = false;

            // Write all the schemaRules that already had ids first to make sure that any newly allocated ids will be
            // unique
            for (SchemaRule rule : existingSchemaRulesToAdd) {
                if (rule.getId() == IndexDescriptor.FORMER_LABEL_SCAN_STORE_ID) {
                    foundNliWithoutId = true;
                } else {
                    dstAccess.writeSchemaRule(rule);
                }
            }

            // Now any indexes that already had ids are in place, and we can get new ids
            if (foundNliWithoutId) {
                dstAccess.writeSchemaRule(NLI_PROTOTYPE.materialise(dstAccess.nextId()));
            }

            if (forceBtreeIndexesToRange) {
                // Forcefully replace non-replaced indexes and constraints
                for (SchemaRule44.Index index : nonReplacedIndexes) {
                    IndexDescriptor rangeIndex = asRangeIndex(index, dstAccess);
                    dstAccess.writeSchemaRule(rangeIndex);
                    toDelete.add(index);
                }
                for (Pair<SchemaRule44.Constraint, SchemaRule44.Index> constraintPair : nonReplacedConstraints) {
                    var oldConstraint = constraintPair.first();
                    var oldIndex = constraintPair.other();
                    var rangeIndex = asRangeIndex(oldIndex, dstAccess);
                    var rangeBackedConstraint =
                            asRangeBackedConstraint(oldConstraint, rangeIndex, dstAccess, dstTokensHolders);
                    rangeIndex = rangeIndex.withOwningConstraintId(rangeBackedConstraint.getId());
                    dstAccess.writeSchemaRule(rangeIndex);
                    dstAccess.writeSchemaRule(rangeBackedConstraint);
                    toDelete.add(oldIndex);
                    toDelete.add(oldConstraint);
                }
            }

            // Don't remove old schemaRules/properties if we have a new store
            if (!shouldCreateNewSchemaStore) {
                for (SchemaRule44 rule : toDelete) {
                    dstAccess.deleteSchemaRule(rule.id());
                }
            }
        }
    }

    static SchemaStore44Migration.SchemaStore44Migrator getSchemaStore44Migrator(
            FileSystemAbstraction fileSystem,
            RecordFormats oldFormat,
            RecordDatabaseLayout directoryLayout,
            CursorContext cursorContext,
            boolean shouldCreateNewSchemaStore,
            boolean forceBtreeIndexesToRange,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            IdGeneratorFactory srcIdGeneratorFactory,
            StoreFactory srcFactory)
            throws IOException {
        try (NeoStores srcStore = srcFactory.openNeoStores(
                        StoreType.SCHEMA,
                        StoreType.PROPERTY,
                        StoreType.LABEL_TOKEN,
                        StoreType.RELATIONSHIP_TYPE_TOKEN,
                        StoreType.PROPERTY_KEY_TOKEN);
                var srcCursors = new CachedStoreCursors(srcStore, cursorContext)) {
            // Need the kernel version from the old metadata store to know if we have injected NLI
            KernelVersion kernelVersion = LegacyMetadataHandler.readMetadata44FromStore(
                            pageCache,
                            directoryLayout.metadataStore(),
                            directoryLayout.getDatabaseName(),
                            cursorContext)
                    .kernelVersion();

            var srcTokensHolders = createTokenHolders(srcStore, srcCursors);
            try (var schemaStore44Reader = getSchemaStore44Reader(
                    fileSystem,
                    directoryLayout,
                    oldFormat,
                    srcIdGeneratorFactory,
                    srcStore,
                    srcTokensHolders,
                    config,
                    pageCache,
                    pageCacheTracer,
                    contextFactory,
                    kernelVersion)) {
                return getSchemaStoreMigration44(
                        schemaStore44Reader,
                        srcCursors,
                        forceBtreeIndexesToRange,
                        shouldCreateNewSchemaStore,
                        srcTokensHolders);
            }
        }
    }

    public static SchemaStore44Migrator getSchemaStoreMigration44(
            SchemaStore44Reader schemaStore44Reader,
            StoreCursors srcCursors,
            boolean forceBtreeIndexesToRange,
            boolean shouldCreateNewSchemaStore,
            TokenHolders srcTokenHolders) {
        var all = schemaStore44Reader.loadAllSchemaRules(srcCursors);
        var toDelete = new ArrayList<SchemaRule44>();
        var toCreate = new ArrayList<SchemaRule>();

        // Organize indexes by SchemaDescriptor

        var indexesBySchema = new HashMap<SchemaDescriptor, List<SchemaRule44.Index>>();
        var uniqueIndexesByName = new HashMap<String, SchemaRule44.Index>();
        var constraintBySchemaAndType = new HashMap<
                SchemaDescriptor, EnumMap<SchemaRule44.ConstraintRuleType, List<SchemaRule44.Constraint>>>();
        for (var schemaRule : all) {
            if (schemaRule instanceof SchemaRule44.Index index) {
                if (!index.unique()) {
                    indexesBySchema
                            .computeIfAbsent(index.schema(), k -> new ArrayList<>())
                            .add(index);
                } else {
                    uniqueIndexesByName.put(index.name(), index);
                }

                if (shouldCreateNewSchemaStore && index.indexType() != SchemaRule44.IndexType.BTREE) {
                    toCreate.add(schemaRule.convertTo50rule());
                }
            }
            if (schemaRule instanceof SchemaRule44.Constraint constraint) {
                boolean indexBacked = constraint.constraintRuleType().isIndexBacked();
                if (indexBacked) {
                    var constraintsByType = constraintBySchemaAndType.computeIfAbsent(
                            constraint.schema(), k -> new EnumMap<>(SchemaRule44.ConstraintRuleType.class));
                    constraintsByType
                            .computeIfAbsent(constraint.constraintRuleType(), k -> new ArrayList<>())
                            .add(constraint);
                }

                if (shouldCreateNewSchemaStore
                        && (!indexBacked || constraint.indexType() == SchemaRule44.IndexType.RANGE)) {
                    toCreate.add(schemaRule.convertTo50rule());
                }
            }
        }

        // Make sure node label index is correctly persisted. There are two situations where it might not be:
        // 1. As part of upgrade to 4.3/4.4 a schema record without any properties was written to the schema store.
        //    This record was used to represent the old label scan store (< 4.3) converted to node label index.
        //    In this case we need to rewrite this schema to give it the properties it should have. In this way we can
        // keep the index id.
        // 2. If no write transaction happened after upgrade of old store to 4.3/4.4 the upgrade transaction was never
        // injected
        //    and node label index (as schema rule with no properties) was never persisted at all. In this case
        //    IndexDescriptor#INJECTED_NLI will be injected by SchemaStore44Reader
        //    when reading schema rules. In this case we materialise this injected rule with a new real id (instead of
        // -2).
        // The ids are selected in migrate, here we just make sure that the rule will always be added later
        List<SchemaRule44.Index> nlis = indexesBySchema.get(SchemaStore44Reader.FORMER_LABEL_SCAN_STORE_SCHEMA);
        if (!shouldCreateNewSchemaStore && nlis != null && !nlis.isEmpty()) {
            SchemaRule44.Index nli = nlis.get(0);
            if (SchemaStore44Reader.FORMER_LABEL_SCAN_STORE_GENERATED_NAME.equals(nli.name())) {
                toCreate.add(nli.convertTo50rule());
            }
        }

        // Figure out which btree indexes that has replacement and can be deleted and which don't
        var nonReplacedIndexes = new ArrayList<SchemaRule44.Index>();
        for (var schema : indexesBySchema.keySet()) {
            List<SchemaRule44.Index> indexes = indexesBySchema.get(schema);
            for (SchemaRule44.Index index : indexes) {
                if (index.indexType() == SchemaRule44.IndexType.BTREE) {
                    if (indexes.size() == 1) {
                        nonReplacedIndexes.add(index);
                    } else {
                        toDelete.add(index);
                    }
                }
            }
        }

        // Figure out which constraints, backed by btree indexes, that has replacement and can be deleted and which
        // don't
        var nonReplacedConstraints = new ArrayList<Pair<SchemaRule44.Constraint, SchemaRule44.Index>>();
        constraintBySchemaAndType.values().stream()
                .flatMap(enumMap -> enumMap.values().stream())
                .forEach(constraintsGroupedBySchemaAndType -> {
                    for (var constraint : constraintsGroupedBySchemaAndType) {
                        var backingIndex = uniqueIndexesByName.remove(constraint.name());
                        if (backingIndex.indexType() == SchemaRule44.IndexType.BTREE) {
                            if (constraintsGroupedBySchemaAndType.size() == 1) {
                                nonReplacedConstraints.add(Pair.of(constraint, backingIndex));
                            } else {
                                toDelete.add(constraint);
                                toDelete.add(backingIndex);
                            }
                        }
                    }
                });

        for (SchemaRule44.Index uniqueIndex : uniqueIndexesByName.values()) {
            // There could be a unique index not linked to a constraint e.g. by crashing in the middle of a constraint
            // creation,
            // since we don't know what constraint type it should be backing we can't know if it is replaced - let's
            // just throw on it instead.
            if (uniqueIndex.indexType() == SchemaRule44.IndexType.BTREE) {
                nonReplacedIndexes.add(uniqueIndex);
            }
        }

        return new SchemaStore44Migrator(
                shouldCreateNewSchemaStore,
                forceBtreeIndexesToRange,
                nonReplacedIndexes,
                nonReplacedConstraints,
                toDelete,
                toCreate,
                srcTokenHolders);
    }

    private static ConstraintDescriptor asRangeBackedConstraint(
            SchemaRule44.Constraint constraint,
            IndexDescriptor rangeIndex,
            SchemaRuleMigrationAccess dstAccess,
            TokenHolders dstTokensHolders) {
        ConstraintDescriptor newConstraint;
        if (constraint.constraintRuleType() == SchemaRule44.ConstraintRuleType.UNIQUE) {
            newConstraint = ConstraintDescriptorFactory.uniqueForSchema(constraint.schema(), rangeIndex.getIndexType());
        } else if (constraint.constraintRuleType() == SchemaRule44.ConstraintRuleType.UNIQUE_EXISTS) {
            newConstraint = ConstraintDescriptorFactory.keyForSchema(constraint.schema(), rangeIndex.getIndexType());
        } else {
            throw new IllegalStateException("We should never see non-index-backed constraint here, but got: "
                    + constraint.userDescription(dstTokensHolders));
        }
        return newConstraint
                .withOwnedIndexId(rangeIndex.getId())
                .withName(constraint.name())
                .withId(dstAccess.nextId());
    }

    private static IndexDescriptor asRangeIndex(SchemaRule44.Index btreeIndex, SchemaRuleMigrationAccess dstAccess) {
        var prototype = btreeIndex.unique()
                ? IndexPrototype.uniqueForSchema(btreeIndex.schema())
                : IndexPrototype.forSchema(btreeIndex.schema());
        return prototype
                .withName(btreeIndex.name())
                .withIndexType(IndexType.RANGE)
                .withIndexProvider(new IndexProviderDescriptor("range", "1.0"))
                .materialise(dstAccess.nextId());
    }

    private static SchemaStore44Reader getSchemaStore44Reader(
            FileSystemAbstraction fileSystem,
            RecordDatabaseLayout recordDatabaseLayout,
            RecordFormats formats,
            IdGeneratorFactory idGeneratorFactory,
            NeoStores neoStores,
            TokenHolders tokenHolders,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            KernelVersion kernelVersion) {
        return new SchemaStore44Reader(
                fileSystem,
                neoStores.getPropertyStore(),
                tokenHolders,
                kernelVersion,
                recordDatabaseLayout.schemaStore(),
                recordDatabaseLayout.idSchemaStore(),
                config,
                SchemaIdType.SCHEMA,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                contextFactory,
                NullLogProvider.getInstance(),
                formats,
                recordDatabaseLayout.getDatabaseName(),
                neoStores.getOpenOptions());
    }
}
