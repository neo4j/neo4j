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
package org.neo4j.storageengine.migration;

import static org.neo4j.storageengine.api.format.MultiVersionedIndexesCompatibility.MULTI_VERSION_INDEXES;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * Migrates token indexes between different neo4j versions. Participates in store upgrade as one of the migration participants.
 * Also takes care of migration from versions before 5.0.
 */
public class TokenIndexMigrator extends AbstractStoreMigrationParticipant {
    private static final String LEGACY_LABEL_INDEX_STORE = "neostore.labelscanstore.db";
    private static final String LEGACY_RELATIONSHIP_TYPE_INDEX_STORE = "neostore.relationshiptypescanstore.db";

    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout layout;
    private final Function<SchemaRule, Path> storeFileProvider;
    private boolean deleteRelationshipTokenIndex;
    private boolean moveFiles;
    private final CursorContextFactory contextFactory;
    private boolean deleteAllIndexes;

    public TokenIndexMigrator(
            String name,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            StorageEngineFactory storageEngineFactory,
            DatabaseLayout layout,
            Function<SchemaRule, Path> storeFileProvider,
            CursorContextFactory contextFactory) {
        super(name);
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.pageCacheTracer = pageCacheTracer;
        this.storageEngineFactory = storageEngineFactory;
        this.layout = layout;
        this.storeFileProvider = storeFileProvider;
        this.contextFactory = contextFactory;
    }

    @Override
    public void migrate(
            DatabaseLayout directoryLayout,
            DatabaseLayout migrationLayout,
            ProgressListener progressListener,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            IndexImporterFactory indexImporterFactory,
            LogTailMetadata tailMetadata) {
        deleteAllIndexes = differentMultiVersionCapabilities(toVersion, fromVersion);
        deleteRelationshipTokenIndex = !fromVersion.hasCompatibleCapabilities(toVersion, CapabilityType.FORMAT);
        // Token indexes were stored at a different location before 5.0
        // This migrator will take them to the 5.0+ location
        moveFiles = scanStoreExists(directoryLayout, LEGACY_LABEL_INDEX_STORE)
                || scanStoreExists(directoryLayout, LEGACY_RELATIONSHIP_TYPE_INDEX_STORE);
    }

    private boolean differentMultiVersionCapabilities(StoreVersion toVersion, StoreVersion fromVersion) {
        return toVersion.hasCapability(MULTI_VERSION_INDEXES) ^ fromVersion.hasCapability(MULTI_VERSION_INDEXES);
    }

    private boolean scanStoreExists(DatabaseLayout directoryLayout, String fileName) {
        return fileSystem.fileExists(directoryLayout.file(fileName));
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            StoreVersion versionToUpgradeFrom,
            StoreVersion versionToMigrateTo)
            throws IOException {
        if (moveFiles) {
            moveTokenIndexes(directoryLayout);
        }

        if (deleteAllIndexes) {
            deleteTokenIndex(directoryLayout, any -> true);
        } else if (deleteRelationshipTokenIndex) {
            deleteTokenIndex(directoryLayout, rule -> rule.schema().entityType() == EntityType.RELATIONSHIP);
        }
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) {
        // nop
    }

    private void moveTokenIndexes(DatabaseLayout databaseLayout) throws IOException {
        for (var schemaRule : storageEngineFactory.loadSchemaRules(
                fileSystem,
                pageCache,
                pageCacheTracer,
                Config.defaults(),
                databaseLayout,
                false,
                r -> r,
                contextFactory)) {
            if (!schemaRule.schema().isAnyTokenSchemaDescriptor()) {
                continue;
            }

            if (schemaRule.schema().entityType() == EntityType.NODE) {
                moveFile(schemaRule, LEGACY_LABEL_INDEX_STORE);
            } else {
                moveFile(schemaRule, LEGACY_RELATIONSHIP_TYPE_INDEX_STORE);
            }
        }
    }

    private void moveFile(SchemaRule schemaRule, String legacyFileName) throws IOException {
        if (scanStoreExists(layout, legacyFileName)) {
            Path destination = storeFileProvider.apply(schemaRule);
            try {
                fileSystem.mkdirs(destination.getParent());
                fileSystem.renameFile(layout.file(legacyFileName), destination);
            } catch (IOException e) {
                throw new IOException("Failed to move LOOKUP index files to index directory during migration", e);
            }
        }
    }

    private void deleteTokenIndex(DatabaseLayout databaseLayout, Predicate<SchemaRule> tokenIndexFilter)
            throws IOException {
        for (var schemaRule : storageEngineFactory.loadSchemaRules(
                fileSystem,
                pageCache,
                pageCacheTracer,
                Config.defaults(),
                databaseLayout,
                false,
                r -> r,
                contextFactory)) {
            if (schemaRule.schema().isAnyTokenSchemaDescriptor() && tokenIndexFilter.test(schemaRule)) {
                Path indexFile = storeFileProvider.apply(schemaRule);
                try {
                    fileSystem.deleteFile(indexFile);
                } catch (IOException e) {
                    throw new IOException("Failed to remove a relationship LOOKUP index file during migration", e);
                }
            }
        }
    }
}
