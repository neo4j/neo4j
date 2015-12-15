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
package org.neo4j.kernel.impl.storemigration.participant;

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
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

/**
 * Migrates schema and label indexes between different neo4j versions.
 * Participates in store upgrade as one of the migration participants.
 * <p>
 * Since index format can be completely incompatible between version should be executed before {@link StoreMigrator}
 */
public class SchemaIndexMigrator extends AbstractStoreMigrationParticipant
{
    private final FileSystemAbstraction fileSystem;
    private boolean deleteObsoleteIndexes = false;
    private File labelIndexDirectory;
    private File schemaIndexDirectory;
    private final SchemaIndexProvider schemaIndexProvider;
    private final LabelScanStoreProvider labelScanStoreProvider;

    public SchemaIndexMigrator( FileSystemAbstraction fileSystem, SchemaIndexProvider schemaIndexProvider,
            LabelScanStoreProvider labelScanStoreProvider )
    {
        super( "Indexes" );
        this.fileSystem = fileSystem;
        this.schemaIndexProvider = schemaIndexProvider;
        this.labelScanStoreProvider = labelScanStoreProvider;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor,
            String versionToMigrateFrom ) throws IOException
    {
        switch ( versionToMigrateFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
        case Legacy20Store.LEGACY_VERSION:
        case Legacy21Store.LEGACY_VERSION:
        case Legacy22Store.LEGACY_VERSION:
        case Legacy23Store.LEGACY_VERSION:
            schemaIndexDirectory = schemaIndexProvider.getSchemaIndexStoreDirectory( storeDir );
            labelIndexDirectory = labelScanStoreProvider.getStoreDirectory( storeDir );
            deleteObsoleteIndexes = true;
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToMigrateFrom );
        }
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
    {
        if ( deleteObsoleteIndexes )
        {
            deleteIndexes( schemaIndexDirectory );
            deleteIndexes( labelIndexDirectory );
        }
    }

    private void deleteIndexes( File indexRootDirectory ) throws IOException
    {
        fileSystem.deleteRecursively( indexRootDirectory );
    }
}
