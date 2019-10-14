/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.io.File;
import java.io.IOException;

import org.neo4j.common.ProgressReporter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
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
    private boolean deleteObsoleteIndexes;
    private File schemaIndexDirectory;
    private final IndexDirectoryStructure indexDirectoryStructure;
    private final StorageEngineFactory storageEngineFactory;

    public SchemaIndexMigrator( String name, FileSystemAbstraction fileSystem, IndexDirectoryStructure indexDirectoryStructure,
            StorageEngineFactory storageEngineFactory )
    {
        super( name );
        this.fileSystem = fileSystem;
        this.indexDirectoryStructure = indexDirectoryStructure;
        this.storageEngineFactory = storageEngineFactory;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo )
    {
        StoreVersion fromVersionInformation = storageEngineFactory.versionInformation( versionToMigrateFrom );
        StoreVersion toVersionInformation = storageEngineFactory.versionInformation( versionToMigrateTo );
        if ( !fromVersionInformation.hasCompatibleCapabilities( toVersionInformation, CapabilityType.INDEX ) )
        {
            schemaIndexDirectory = indexDirectoryStructure.rootDirectory();
            if ( schemaIndexDirectory != null )
            {
                deleteObsoleteIndexes = true;
            }
            // else this schema index provider doesn't have any persistent storage to delete.
        }
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom,
            String versionToMigrateTo ) throws IOException
    {
        if ( deleteObsoleteIndexes )
        {
            deleteIndexes( schemaIndexDirectory );
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )
    {
        // nop
    }

    private void deleteIndexes( File indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }
}
