/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.storageengine.migration;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.common.EntityType;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * Migrates schema and label indexes between different neo4j versions.
 * Participates in store upgrade as one of the migration participants.
 * <p>
 * Since index format can be completely incompatible between version should be executed before org.neo4j.kernel.impl.storemigration.StoreMigrator
 */
public class SchemaIndexMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final IndexDirectoryStructure indexDirectoryStructure;
    private final StorageEngineFactory storageEngineFactory;
    private final boolean checkIndexCapabilities;
    private boolean deleteAllIndexes;
    private boolean deleteRelationshipIndexes;

    public SchemaIndexMigrator( String name, FileSystemAbstraction fileSystem, PageCache pageCache, IndexDirectoryStructure indexDirectoryStructure,
            StorageEngineFactory storageEngineFactory, boolean checkIndexCapabilities )
    {
        super( name );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.indexDirectoryStructure = indexDirectoryStructure;
        this.storageEngineFactory = storageEngineFactory;
        this.checkIndexCapabilities = checkIndexCapabilities;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
                         String versionToMigrateFrom, String versionToMigrateTo, IndexImporterFactory indexImporterFactory )
    {
        StoreVersion fromVersion = storageEngineFactory.versionInformation( versionToMigrateFrom );
        StoreVersion toVersion = storageEngineFactory.versionInformation( versionToMigrateTo );

        deleteAllIndexes = checkIndexCapabilities && !fromVersion.hasCompatibleCapabilities( toVersion, CapabilityType.INDEX );
        deleteRelationshipIndexes = !fromVersion.hasCompatibleCapabilities( toVersion, CapabilityType.FORMAT );
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom,
            String versionToMigrateTo ) throws IOException
    {
        Path schemaIndexDirectory = indexDirectoryStructure.rootDirectory();
        if ( schemaIndexDirectory != null )
        {
            if ( deleteAllIndexes )
            {
                deleteIndexes( schemaIndexDirectory );
            }
            else if ( deleteRelationshipIndexes )
            {
                deleteRelationshipIndexes( directoryLayout );
            }
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )
    {
        // nop
    }

    private void deleteIndexes( Path indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }

    private void deleteRelationshipIndexes( DatabaseLayout databaseLayout ) throws IOException
    {
        for ( SchemaRule schemaRule : storageEngineFactory.loadSchemaRules( fileSystem, pageCache, Config.defaults(), databaseLayout, CursorContext.NULL ) )
        {
            if ( schemaRule.schema().entityType() == EntityType.RELATIONSHIP )
            {
                fileSystem.deleteRecursively( indexDirectoryStructure.directoryForIndex( schemaRule.getId() ) );
            }
        }
    }
}
