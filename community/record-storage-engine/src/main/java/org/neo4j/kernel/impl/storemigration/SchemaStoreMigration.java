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
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.RecordStorageMigrator.createTokenHolders;
import static org.neo4j.kernel.impl.storemigration.RecordStorageMigrator.need50Migration;
import static org.neo4j.kernel.impl.storemigration.SchemaStore44Migration.getSchemaStore44Migrator;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;

import java.io.IOException;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;

public class SchemaStoreMigration {
    public interface SchemaStoreMigrator {
        void assertCanMigrate() throws IllegalStateException;

        void copyFilesInPreparationForMigration(
                FileSystemAbstraction fileSystem,
                RecordDatabaseLayout directoryLayout,
                RecordDatabaseLayout migrationLayout)
                throws IOException;

        void migrate(SchemaRuleMigrationAccess dstAccess, TokenHolders dstTokensHolders) throws KernelException;
    }

    record SchemaStoreNewFamilyMigration(List<SchemaRule> rules) implements SchemaStoreMigrator {

        @Override
        public void assertCanMigrate() throws IllegalStateException {
            // Can always migrate
        }

        @Override
        public void copyFilesInPreparationForMigration(
                FileSystemAbstraction fileSystem,
                RecordDatabaseLayout directoryLayout,
                RecordDatabaseLayout migrationLayout)
                throws IOException {
            // Migration with the batch importer would have copied the property, property key token, and property key
            // name stores
            // into the migration directory, which is needed for the schema store migration. However, it might choose to
            // skip
            // store files that it did not change, or didn't migrate. It could also be that we didn't do a normal store
            // format migration. Then those files will be missing and the schema store migration would create empty ones
            // that
            // ended up overwriting the real ones. Those are then deleted by the migration afterwards, to avoid
            // overwriting the
            // actual files in the final copy from the migration directory, to the real store directory. When do a
            // schema store
            // migration, we will be reading and writing properties, and property key tokens, so we need those files.
            // To get them, we just copy them again with the SKIP strategy, so we avoid overwriting any files that might
            // have
            // been migrated earlier.
            List<DatabaseFile> databaseFiles = asList(
                    RecordDatabaseFile.PROPERTY_STORE,
                    RecordDatabaseFile.PROPERTY_ARRAY_STORE,
                    RecordDatabaseFile.PROPERTY_STRING_STORE,
                    RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                    RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                    RecordDatabaseFile.LABEL_TOKEN_STORE,
                    RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                    RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                    RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE);
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
            for (SchemaRule rule : rules) {
                dstAccess.writeSchemaRule(rule);
            }
        }
    }

    static class SchemaStoreNoMigration implements SchemaStoreMigrator {
        @Override
        public void assertCanMigrate() throws IllegalStateException {
            // Can always migrate
        }

        @Override
        public void copyFilesInPreparationForMigration(
                FileSystemAbstraction fileSystem,
                RecordDatabaseLayout directoryLayout,
                RecordDatabaseLayout migrationLayout) {
            // No-op
        }

        @Override
        public void migrate(SchemaRuleMigrationAccess dstAccess, TokenHolders dstTokensHolders) {
            // No-op - only need migration if changing family.
        }
    }

    static SchemaStoreMigrator getSchemaStoreMigration(
            RecordFormats oldFormat,
            RecordDatabaseLayout directoryLayout,
            CursorContext cursorContext,
            boolean requiresPropertyMigration,
            boolean forceBtreeIndexesToRange,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            FileSystemAbstraction fileSystem,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        IdGeneratorFactory srcIdGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();
        StoreFactory srcFactory = createStoreFactory(
                directoryLayout,
                config,
                srcIdGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fileSystem,
                oldFormat,
                contextFactory);

        if (!need50Migration(oldFormat)) {
            if (!requiresPropertyMigration) {
                // No schema store migration needed if we are not changing format family (or migrating properties for
                // other reason)
                return new SchemaStoreNoMigration();
            }

            List<SchemaRule> schemaRules;
            try (NeoStores srcStore = srcFactory.openNeoStores(
                            StoreType.PROPERTY_KEY_TOKEN,
                            StoreType.PROPERTY_KEY_TOKEN_NAME,
                            StoreType.LABEL_TOKEN,
                            StoreType.LABEL_TOKEN_NAME,
                            StoreType.RELATIONSHIP_TYPE_TOKEN,
                            StoreType.RELATIONSHIP_TYPE_TOKEN_NAME,
                            StoreType.PROPERTY,
                            StoreType.PROPERTY_STRING,
                            StoreType.PROPERTY_ARRAY,
                            StoreType.SCHEMA);
                    var srcCursors = new CachedStoreCursors(srcStore, cursorContext)) {
                TokenHolders srcTokenHolders = createTokenHolders(srcStore, srcCursors, memoryTracker);
                org.neo4j.internal.recordstorage.SchemaStorage schemaStorage =
                        new org.neo4j.internal.recordstorage.SchemaStorage(srcStore.getSchemaStore(), srcTokenHolders);

                schemaRules = Iterables.asList(schemaStorage.getAll(srcCursors, memoryTracker));
            }
            return new SchemaStoreNewFamilyMigration(schemaRules);
        }

        // 4.4 stores, need special handling for the schema store
        return getSchemaStore44Migrator(
                fileSystem,
                oldFormat,
                directoryLayout,
                cursorContext,
                requiresPropertyMigration,
                forceBtreeIndexesToRange,
                config,
                pageCache,
                pageCacheTracer,
                contextFactory,
                srcIdGeneratorFactory,
                srcFactory,
                memoryTracker);
    }

    private static StoreFactory createStoreFactory(
            RecordDatabaseLayout databaseLayout,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            FileSystemAbstraction fileSystem,
            RecordFormats formats,
            CursorContextFactory contextFactory) {
        return new StoreFactory(
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fileSystem,
                formats,
                NullLogProvider.getInstance(),
                contextFactory,
                true,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                immutable.empty());
    }
}
