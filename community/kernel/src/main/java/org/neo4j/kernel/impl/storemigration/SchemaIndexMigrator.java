/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;

public class SchemaIndexMigrator implements StoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;

    public SchemaIndexMigrator( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
            LabelScanStoreProvider labelScanStoreProvider, String versionToMigrateFrom ) throws IOException
    {
        switch ( versionToMigrateFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
        case Legacy20Store.LEGACY_VERSION:
        case Legacy21Store.LEGACY_VERSION:
        case Legacy22Store.LEGACY_VERSION:
        case Legacy23Store.LEGACY_VERSION:
            deleteIndexes( storeDir, schemaIndexProvider, labelScanStoreProvider );
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToMigrateFrom );
        }
    }

    private void deleteIndexes( File storeDir, SchemaIndexProvider schemaIndexProvider,
            LabelScanStoreProvider labelScanStoreProvider ) throws IOException
    {
        deleteIndexes( schemaIndexProvider.getStoreDirectory( storeDir ) );
        deleteIndexes( labelScanStoreProvider.getStoreDirectory( storeDir ) );
    }

    private void deleteIndexes( File indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
    { // nothing to do
    }

    @Override
    public void rebuildCounts( File storeDir, String versionToMigrateFrom ) throws IOException
    { // nothing to do
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    { // nothing to do
    }
}
