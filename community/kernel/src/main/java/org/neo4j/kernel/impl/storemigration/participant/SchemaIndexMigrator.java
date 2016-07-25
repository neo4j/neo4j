/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
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
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        RecordFormats from = RecordFormatSelector.selectForVersion( versionToMigrateFrom );
        RecordFormats to = RecordFormatSelector.selectForVersion( versionToMigrateTo );
        if ( !from.hasSameCapabilities( to, CapabilityType.INDEX ) )
        {
            schemaIndexDirectory = schemaIndexProvider.getSchemaIndexStoreDirectory( storeDir );
            labelIndexDirectory = labelScanStoreProvider.getStoreDirectory( storeDir );
            deleteObsoleteIndexes = true;
        }
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
            String versionToMigrateTo ) throws IOException
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
