/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.storemigration.legacystore.v34.Legacy34Store;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.string.UTF8;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.neo4j.io.fs.IoPrimitiveUtils.readAndFlip;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public final class MigrationTestUtils
{
    private MigrationTestUtils()
    {
    }

    public static void changeVersionNumber( FileSystemAbstraction fileSystem, File storeFile, String versionString )
            throws IOException
    {
        byte[] versionBytes = UTF8.encode( versionString );
        try ( StoreChannel fileChannel = fileSystem.write( storeFile ) )
        {
            fileChannel.position( fileSystem.getFileSize( storeFile ) - versionBytes.length );
            fileChannel.writeAll( ByteBuffer.wrap( versionBytes ) );
        }
    }

    public static void prepareSampleLegacyDatabase( String version, FileSystemAbstraction workingFs,
            Path workingDirectory, Path prepareDirectory ) throws IOException
    {
        if ( Files.notExists( prepareDirectory ) )
        {
            throw new IllegalArgumentException( "bad prepare directory" );
        }
        Path resourceDirectory = findFormatStoreDirectoryForVersion( version, prepareDirectory );
        workingFs.deleteRecursively( workingDirectory.toFile() );
        workingFs.mkdirs( workingDirectory.toFile() );
        workingFs.copyRecursively( resourceDirectory.toFile(), workingDirectory.toFile() );
    }

    public static Path findFormatStoreDirectoryForVersion( String version, Path targetDir ) throws IOException
    {
        if ( StandardV3_4.STORE_VERSION.equals( version ) )
        {
            return find34FormatStoreDirectory( targetDir );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown version" );
        }
    }

    private static Path find34FormatStoreDirectory( Path targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy34Store.class, "upgradeTest34Db.zip", targetDir );
    }

    public static boolean checkNeoStoreHasFormatVersion( StoreVersionCheck check, RecordFormats expectedFormat )
    {
        String expectedVersion = expectedFormat.storeVersion();
        boolean successful = check.checkUpgrade( expectedVersion, PageCursorTracer.NULL ).outcome.isSuccessful();
        if ( successful )
        {
            String storeVersion = check.storeVersion( PageCursorTracer.NULL )
                    .orElseThrow( () -> new RuntimeException( "Expected store to have a store version." ) );
            return expectedVersion.equals( storeVersion );
        }
        return false;
    }

    public static void verifyFilesHaveSameContent( FileSystemAbstraction fileSystem, Path original, Path other )
            throws IOException
    {
        final int bufferBatchSize = 32 * 1024;
        Path[] files = Arrays.stream( fileSystem.listFiles( original.toFile() ) ).map( File::toPath ).toArray( Path[]::new );
        for ( Path originalFile : files )
        {
            Path otherFile = other.resolve( originalFile.getFileName() );
            if ( !fileSystem.isDirectory( originalFile.toFile() ) )
            {
                try ( StoreChannel originalChannel = fileSystem.read( originalFile.toFile() );
                      StoreChannel otherChannel = fileSystem.read( otherFile.toFile() ) )
                {
                    ByteBuffer buffer = ByteBuffers.allocate( bufferBatchSize, INSTANCE );
                    while ( true )
                    {
                        if ( !readAndFlip( originalChannel, buffer, bufferBatchSize ) )
                        {
                            break;
                        }
                        byte[] originalBytes = new byte[buffer.limit()];
                        buffer.get( originalBytes );

                        if ( !readAndFlip( otherChannel, buffer, bufferBatchSize ) )
                        {
                            fail( "Files have different sizes" );
                        }

                        byte[] otherBytes = new byte[buffer.limit()];
                        buffer.get( otherBytes );

                        assertArrayEquals( "Different content in " + originalFile, originalBytes, otherBytes );
                    }
                }
            }
        }
    }
}
