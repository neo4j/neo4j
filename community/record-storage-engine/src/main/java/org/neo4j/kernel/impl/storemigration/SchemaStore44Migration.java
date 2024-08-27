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
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaRule;
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
import org.neo4j.memory.MemoryTracker;
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
            SchemaStore44MigrationUtil.assertCanMigrate(
                    forceBtreeIndexesToRange, nonReplacedIndexes, nonReplacedConstraints, srcTokenHolders);
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
                    IndexDescriptor rangeIndex = SchemaStore44MigrationUtil.asRangeIndex(index, dstAccess::nextId);
                    dstAccess.writeSchemaRule(rangeIndex);
                    toDelete.add(index);
                }
                for (Pair<SchemaRule44.Constraint, SchemaRule44.Index> constraintPair : nonReplacedConstraints) {
                    var oldConstraint = constraintPair.first();
                    var oldIndex = constraintPair.other();
                    var rangeIndex = SchemaStore44MigrationUtil.asRangeIndex(oldIndex, dstAccess::nextId);
                    var rangeBackedConstraint = SchemaStore44MigrationUtil.asRangeBackedConstraint(
                            oldConstraint, rangeIndex, dstAccess::nextId, dstTokensHolders);
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
            StoreFactory srcFactory,
            MemoryTracker memoryTracker)
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

            var srcTokensHolders = createTokenHolders(srcStore, srcCursors, memoryTracker);
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
                        srcTokensHolders,
                        memoryTracker);
            }
        }
    }

    public static SchemaStore44Migrator getSchemaStoreMigration44(
            SchemaStore44Reader schemaStore44Reader,
            StoreCursors srcCursors,
            boolean forceBtreeIndexesToRange,
            boolean shouldCreateNewSchemaStore,
            TokenHolders srcTokenHolders,
            MemoryTracker memoryTracker) {
        var all = schemaStore44Reader.loadAllSchemaRules(srcCursors, memoryTracker);

        SchemaStore44MigrationUtil.SchemaInfo44 schemaInfo44 =
                SchemaStore44MigrationUtil.extractRuleInfo(shouldCreateNewSchemaStore, all);

        return new SchemaStore44Migrator(
                shouldCreateNewSchemaStore,
                forceBtreeIndexesToRange,
                schemaInfo44.nonReplacedIndexes(),
                schemaInfo44.nonReplacedConstraints(),
                schemaInfo44.toDelete(),
                schemaInfo44.toCreate(),
                srcTokenHolders);
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
