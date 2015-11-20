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

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaIndexMigratorTest
{
    private final FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
    private final SchemaIndexProvider schemaIndexProvider = mock( SchemaIndexProvider.class );
    private final LabelScanStoreProvider labelScanStoreProvider = mock( LabelScanStoreProvider.class );
    private final File storeDir = new File( "store" );
    private final File migrationDir = new File( "migrationDir" );


    private final SchemaIndexMigrator migrator = new SchemaIndexMigrator( fs );

    @Test
    public void schemaAndLabelIndexesRemovedAfterSuccessfulMigration() throws IOException
    {
        when( schemaIndexProvider.getProviderDescriptor() )
                .thenReturn( new SchemaIndexProvider.Descriptor( "key", "version" ) );

        migrator.migrate( storeDir, migrationDir, schemaIndexProvider, labelScanStoreProvider,
                Legacy23Store.LEGACY_VERSION );

        migrator.moveMigratedFiles( migrationDir, storeDir, Legacy23Store.LEGACY_VERSION );

        verify( fs ).deleteRecursively( schemaIndexProvider.getStoreDirectory( storeDir ) );
        verify( fs ).deleteRecursively( labelScanStoreProvider.getStoreDirectory( storeDir ) );
    }
}
