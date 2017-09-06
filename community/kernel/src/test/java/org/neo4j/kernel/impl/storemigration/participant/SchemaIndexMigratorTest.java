/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaIndexMigratorTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final MigrationProgressMonitor.Section progressMonitor = mock( MigrationProgressMonitor.Section.class );
    private final SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private final File storeDir = new File( "store" );
    private final File migrationDir = new File( "migrationDir" );

    private final SchemaIndexMigrator migrator = new SchemaIndexMigrator( fs, schemaIndexProvider );

    @Test
    public void schemaAndLabelIndexesRemovedAfterSuccessfulMigration() throws IOException
    {
        IndexDirectoryStructure directoryStructure = mock( IndexDirectoryStructure.class );
        File indexProviderRootDirectory = new File( storeDir, "just-some-directory" );
        when( directoryStructure.rootDirectory() ).thenReturn( indexProviderRootDirectory );
        when( schemaIndexProvider.directoryStructure() ).thenReturn( directoryStructure );
        when( schemaIndexProvider.getProviderDescriptor() )
                .thenReturn( new SchemaIndexProvider.Descriptor( "key", "version" ) );

        migrator.migrate( storeDir, migrationDir, progressMonitor, StandardV2_3.STORE_VERSION,
                StandardV3_0.STORE_VERSION );

        migrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_0.STORE_VERSION );

        verify( fs ).deleteRecursively( indexProviderRootDirectory );
    }
}
