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
package org.neo4j.kernel.api.impl.index;

import static org.neo4j.storageengine.api.format.MultiVersionedIndexesCompatibility.MULTI_VERSION_INDEXES;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;

/**
 * Migrates schema and label indexes between different neo4j versions.
 * Participates in store upgrade as one of the migration participants.
 * <p>
 * Since index format can be completely incompatible between version should be executed before org.neo4j.kernel.impl.storemigration.StoreMigrator
 */
public class SchemaIndexMigrator extends AbstractStoreMigrationParticipant {
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final IndexDirectoryStructure indexDirectoryStructure;
    private final StorageEngineFactory storageEngineFactory;
    private final CursorContextFactory contextFactory;
    private boolean deleteRelationshipIndexes;
    private boolean deleteAllIndexes;

    public SchemaIndexMigrator(
            String name,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            IndexDirectoryStructure indexDirectoryStructure,
            StorageEngineFactory storageEngineFactory,
            CursorContextFactory contextFactory) {
        super(name);
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.pageCacheTracer = pageCacheTracer;
        this.indexDirectoryStructure = indexDirectoryStructure;
        this.storageEngineFactory = storageEngineFactory;
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
        deleteRelationshipIndexes = !fromVersion.hasCompatibleCapabilities(toVersion, CapabilityType.FORMAT);
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            StoreVersion versionToUpgradeFrom,
            StoreVersion versionToMigrateTo)
            throws IOException {
        Path schemaIndexDirectory = indexDirectoryStructure.rootDirectory();
        if (schemaIndexDirectory != null) {
            if (deleteAllIndexes) {
                fileSystem.deleteRecursively(schemaIndexDirectory);
            } else if (deleteRelationshipIndexes) {
                deleteRelationshipIndexes(directoryLayout);
            }
        }
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) {
        // nop
    }

    private boolean differentMultiVersionCapabilities(StoreVersion toVersion, StoreVersion fromVersion) {
        return toVersion.hasCapability(MULTI_VERSION_INDEXES) ^ fromVersion.hasCapability(MULTI_VERSION_INDEXES);
    }

    private void deleteRelationshipIndexes(DatabaseLayout databaseLayout) throws IOException {
        for (SchemaRule schemaRule : storageEngineFactory.loadSchemaRules(
                fileSystem,
                pageCache,
                pageCacheTracer,
                Config.defaults(),
                databaseLayout,
                false,
                r -> r,
                contextFactory)) {
            if (schemaRule.schema().entityType() == EntityType.RELATIONSHIP) {
                fileSystem.deleteRecursively(indexDirectoryStructure.directoryForIndex(schemaRule.getId()));
            }
        }
    }
}
