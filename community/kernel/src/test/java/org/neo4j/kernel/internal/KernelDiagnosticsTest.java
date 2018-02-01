/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.internal;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.AssertableLogProvider;

public class KernelDiagnosticsTest
{
    @Test
    public void shouldPrintDiskUsage()
    {
        File storeDir = Mockito.mock( File.class );
        Mockito.when( storeDir.getTotalSpace() ).thenReturn( 100L );
        Mockito.when( storeDir.getFreeSpace() ).thenReturn( 40L );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        KernelDiagnostics.StoreFiles storeFiles = new KernelDiagnostics.StoreFiles( storeDir );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.assertContainsMessageContaining( "100 / 40 / 40" );
    }

    @Test
    public void shouldCountFileSizeRecursively()
    {
        // Mock a file structure:
        //   storeDir/indexDir/indexFile (1 kB)
        //   storeDir/neostore (3 kB)

        File indexFile = Mockito.mock( File.class );
        Mockito.when( indexFile.isDirectory() ).thenReturn( false );
        Mockito.when( indexFile.getName() ).thenReturn( "indexFile" );
        Mockito.when( indexFile.length() ).thenReturn( 1024L );

        File indexDir = Mockito.mock( File.class );
        Mockito.when( indexDir.isDirectory() ).thenReturn( true );
        Mockito.when( indexDir.listFiles()).thenReturn( new File[] {indexFile} );
        Mockito.when( indexDir.getName() ).thenReturn( "indexDir" );

        File dbFile = Mockito.mock( File.class );
        Mockito.when( dbFile.isDirectory() ).thenReturn( false );
        Mockito.when( dbFile.getName() ).thenReturn( MetaDataStore.DEFAULT_NAME );
        Mockito.when( dbFile.length() ).thenReturn( 3 * 1024L );

        File storeDir = Mockito.mock( File.class );
        Mockito.when( storeDir.isDirectory() ).thenReturn( true );
        Mockito.when( storeDir.listFiles()).thenReturn( new File[] {indexDir, dbFile} );
        Mockito.when( storeDir.getName() ).thenReturn( "storeDir" );
        Mockito.when( storeDir.getAbsolutePath() ).thenReturn( "/test/storeDir" );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        KernelDiagnostics.StoreFiles storeFiles = new KernelDiagnostics.StoreFiles( storeDir );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.assertContainsMessageContaining( "Total size of store: 4.00 kB" );
        logProvider.assertContainsMessageContaining( "Total size of mapped files: 3.00 kB" );
    }
}
