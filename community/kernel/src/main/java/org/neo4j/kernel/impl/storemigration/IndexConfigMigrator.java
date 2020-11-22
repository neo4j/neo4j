/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;

public class IndexConfigMigrator extends AbstractStoreMigrationParticipant
{
    private static final String INDEX_CONFIG_MIGRATION_TAG = "indexConfigMigration";
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCache pageCache;
    private final LogService logService;
    private final StorageEngineFactory storageEngineFactory;
    private final IndexProviderMap indexProviderMap;
    private final Log log;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    IndexConfigMigrator( FileSystemAbstraction fs, Config config, PageCache pageCache, LogService logService, StorageEngineFactory storageEngineFactory,
            IndexProviderMap indexProviderMap, Log log, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        super( "Index config" );
        this.fs = fs;
        this.config = config;
        this.pageCache = pageCache;
        this.logService = logService;
        this.storageEngineFactory = storageEngineFactory;
        this.indexProviderMap = indexProviderMap;
        this.log = log;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progress, String versionToMigrateFrom,
            String versionToMigrateTo ) throws IOException, KernelException
    {
        if ( needConfigMigration( versionToMigrateFrom, versionToMigrateTo ) )
        {
            migrateIndexConfigs( directoryLayout, migrationLayout, versionToMigrateTo );
        }
    }

    private void migrateIndexConfigs( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, String versionToMigrateTo )
            throws IOException, KernelException
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_CONFIG_MIGRATION_TAG );
                var ruleAccess = storageEngineFactory.schemaRuleMigrationAccess( fs, pageCache, config, migrationLayout, logService,
                versionToMigrateTo, pageCacheTracer, cursorTracer, memoryTracker ) )
        {
            for ( SchemaRule rule : ruleAccess.getAll() )
            {
                SchemaRule upgraded = migrateIndexConfig( rule, directoryLayout, fs, pageCache, indexProviderMap, log, cursorTracer );

                if ( upgraded != rule )
                {
                    ruleAccess.writeSchemaRule( upgraded );
                }
            }
        }
    }

    public static SchemaRule migrateIndexConfig( SchemaRule rule, DatabaseLayout directoryLayout, FileSystemAbstraction fs, PageCache pageCache,
            IndexProviderMap indexProviderMap, Log log, PageCursorTracer cursorTracer ) throws IOException
    {
        if ( rule instanceof IndexDescriptor )
        {
            IndexDescriptor old = (IndexDescriptor) rule;
            long indexId = old.getId();
            IndexProviderDescriptor provider = old.getIndexProvider();

            IndexMigration indexMigration = IndexMigration.migrationFromOldProvider( provider.getKey(), provider.getVersion() );

            IndexConfig indexConfig = indexMigration.extractIndexConfig( fs, pageCache, directoryLayout, indexId, cursorTracer, log );

            IndexDescriptor newIndexReference = old.withIndexConfig( indexConfig );
            IndexProvider indexProvider = indexProviderMap.lookup( indexMigration.desiredAlternativeProvider );
            return indexProvider.completeConfiguration( newIndexReference );
        }
        return rule;
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToMigrateFrom, String versionToMigrateTo )
            throws IOException
    {
        if ( needConfigMigration( versionToMigrateFrom, versionToMigrateTo ) )
        {
            for ( IndexMigration indexMigration : IndexMigration.nonRetired() )
            {
                for ( Path nonRetiredRootDirectory : indexMigration.providerRootDirectories( directoryLayout ) )
                {
                    fs.deleteRecursively( nonRetiredRootDirectory );
                }
            }
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )

    {
        // Nothing to clean up
    }

    private boolean needConfigMigration( String versionToMigrateFrom, String versionToMigrateTo )
    {
        StoreVersion fromVersionInformation = storageEngineFactory.versionInformation( versionToMigrateFrom );
        StoreVersion toVersionInformation = storageEngineFactory.versionInformation( versionToMigrateTo );
        return !fromVersionInformation.hasCompatibleCapabilities( toVersionInformation, CapabilityType.INDEX_CONFIG );
    }
}
