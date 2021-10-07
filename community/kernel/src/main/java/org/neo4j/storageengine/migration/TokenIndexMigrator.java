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

import org.neo4j.common.ProgressReporter;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.CapabilityType;

/**
 * Migrates token indexes between different neo4j versions. Participates in store upgrade as one of the migration participants.
 */
public class TokenIndexMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private final StorageEngineFactory storageEngineFactory;
    private final DatabaseLayout layout;
    private boolean deleteRelationshipTokenIndex;

    public TokenIndexMigrator( String name, FileSystemAbstraction fileSystem, StorageEngineFactory storageEngineFactory, DatabaseLayout layout )
    {
        super( name );
        this.fileSystem = fileSystem;
        this.storageEngineFactory = storageEngineFactory;
        this.layout = layout;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
                         String versionToMigrateFrom, String versionToMigrateTo, IndexImporterFactory indexImporterFactory )
    {
        StoreVersion fromVersion = storageEngineFactory.versionInformation( versionToMigrateFrom );
        StoreVersion toVersion = storageEngineFactory.versionInformation( versionToMigrateTo );
        deleteRelationshipTokenIndex = !fromVersion.hasCompatibleCapabilities( toVersion, CapabilityType.FORMAT );
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom, String versionToMigrateTo ) throws IOException
    {
        if ( deleteRelationshipTokenIndex )
        {
            fileSystem.deleteFile( layout.relationshipTypeScanStore() );
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout )
    {
        // nop
    }
}
