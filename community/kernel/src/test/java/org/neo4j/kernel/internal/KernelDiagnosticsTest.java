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
package org.neo4j.kernel.internal;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.neo4j.io.ByteUnit.kibiBytes;

public class KernelDiagnosticsTest
{
    @Rule
    public final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldPrintDiskUsage()
    {
        // Not sure how to get around this w/o spying. The method that we're unit testing will construct
        // other File instances with this guy as parent and internally the File constructor uses the field 'path'
        // which, if purely mocked, won't be assigned. At the same time we want to control the total/free space methods
        // and what they return... a tough one.
        File storeDir = Mockito.spy( new File( "storeDir" ) );
        Mockito.when( storeDir.getTotalSpace() ).thenReturn( 100L );
        Mockito.when( storeDir.getFreeSpace() ).thenReturn( 40L );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        KernelDiagnostics.StoreFiles storeFiles = new KernelDiagnostics.StoreFiles( storeDir );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.assertContainsMessageContaining( "100 / 40 / 40" );
    }

    @Test
    public void shouldCountFileSizeRecursively() throws IOException
    {
        // file structure:
        //   storeDir/indexDir/indexFile (1 kB)
        //   storeDir/neostore (3 kB)
        File storeDir = directory.directory( "storeDir" );
        File indexDir = directory( storeDir, "indexDir" );
        file( indexDir, "indexFile", (int) kibiBytes( 1 ) );
        file( storeDir, MetaDataStore.DEFAULT_NAME, (int) kibiBytes( 3 ) );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        KernelDiagnostics.StoreFiles storeFiles = new KernelDiagnostics.StoreFiles( storeDir );
        storeFiles.dump( logProvider.getLog( getClass() ).debugLogger() );

        logProvider.assertContainsMessageContaining( "Total size of store: 4.00 kB" );
        logProvider.assertContainsMessageContaining( "Total size of mapped files: 3.00 kB" );
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
        try ( StoreChannel channel = fs.create( file ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( size );
            buffer.position( size ).flip();
            channel.write( buffer );
        }
        return file;
    }
}
