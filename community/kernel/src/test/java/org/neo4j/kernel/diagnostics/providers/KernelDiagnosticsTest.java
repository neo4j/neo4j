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
package org.neo4j.kernel.diagnostics.providers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.ByteUnit.kibiBytes;

@ExtendWith( DefaultFileSystemExtension.class )
@Neo4jLayoutExtension
class KernelDiagnosticsTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void shouldPrintDiskUsage() throws IOException
    {
        // Not sure how to get around this w/o spying. The method that we're unit testing will construct
        // other File instances with this guy as parent and internally the File constructor uses the field 'path'
        // which, if purely mocked, won't be assigned. At the same time we want to control the total/free space methods
        // and what they return... a tough one.
        File storeDir = Mockito.spy( new File( "storeDir" ) );
        DatabaseLayout layout = mock( DatabaseLayout.class );
        when( layout.databaseDirectory() ).thenReturn( storeDir );
        when( storeDir.getTotalSpace() ).thenReturn( 100L );
        when( storeDir.getFreeSpace() ).thenReturn( 40L );
        StorageEngineFactory storageEngineFactory = mock( StorageEngineFactory.class );
        when( storageEngineFactory.listStorageFiles( any(), any() ) ).thenReturn( Collections.emptyList() );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        StoreFilesDiagnostics storeFiles = new StoreFilesDiagnostics( storageEngineFactory, fs, layout );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.rawMessageMatcher().assertContains( "100 / 40 / 40" );
    }

    @Test
    void printDatabaseFileStoreType()
    {
        StorageEngineFactory storageEngineFactory = mock( StorageEngineFactory.class );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        StoreFilesDiagnostics storeFiles = new StoreFilesDiagnostics( storageEngineFactory, fs, databaseLayout );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.rawMessageMatcher().assertContains( "Storage files stored on file store: " );
    }

    @Test
    void shouldCountFileSizeRecursively() throws IOException
    {
        // file structure:
        //   storeDir/indexDir/indexFile (1 kB)
        //   storeDir/neostore (3 kB)
        File storeDir = testDirectory.directory( "storeDir" );
        DatabaseLayout layout = DatabaseLayout.ofFlat( storeDir );
        File indexDir = directory( storeDir, "indexDir" );
        file( indexDir, "indexFile", (int) kibiBytes( 1 ) );
        file( storeDir, layout.metadataStore().getName(), (int) kibiBytes( 3 ) );
        StorageEngineFactory storageEngineFactory = mock( StorageEngineFactory.class );
        when( storageEngineFactory.listStorageFiles( any(), any() ) ).thenReturn( Arrays.asList( layout.metadataStore() ) );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        StoreFilesDiagnostics storeFiles = new StoreFilesDiagnostics( storageEngineFactory, fs, layout );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.rawMessageMatcher().assertContains( "Total size of store: 4.000KiB" );
        logProvider.rawMessageMatcher().assertContains( "Total size of mapped files: 3.000KiB" );
    }

    private File directory( File parent, String name ) throws IOException
    {
        File dir = new File( parent, name );
        fs.mkdirs( dir );
        return dir;
    }

    private File file( File parent, String name, int size ) throws IOException
    {
        File file = new File( parent, name );
        try ( StoreChannel channel = fs.write( file ) )
        {
            ByteBuffer buffer = ByteBuffers.allocate( size );
            buffer.position( size ).flip();
            channel.write( buffer );
        }
        return file;
    }
}
