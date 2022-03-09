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
package org.neo4j.kernel.impl.storemigration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.kernel.impl.storemigration.legacystore.v43.Legacy43Store;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.string.UTF8;
import org.neo4j.test.Unzip;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.DEFAULT_FILENAME_FILTER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public final class MigrationTestUtils
{
    private MigrationTestUtils()
    {
    }

    static void changeVersionNumber( FileSystemAbstraction fileSystem, Path storeFile, String versionString ) throws IOException
    {
        byte[] versionBytes = UTF8.encode( versionString );
        try ( StoreChannel fileChannel = fileSystem.write( storeFile ) )
        {
            fileChannel.position( fileSystem.getFileSize( storeFile ) - versionBytes.length );
            fileChannel.writeAll( ByteBuffer.wrap( versionBytes ) );
        }
    }

    public static void prepareSampleLegacyDatabase( String version, FileSystemAbstraction fs,
            RecordDatabaseLayout databaseLayout, Path prepareDirectory ) throws IOException
    {
        if ( Files.notExists( prepareDirectory ) )
        {
            throw new IllegalArgumentException( "bad prepare directory" );
        }
        Path source = findFormatStoreDirectoryForVersion( version, prepareDirectory );
        Path dbDirectory = databaseLayout.databaseDirectory();
        Path txDirectory = databaseLayout.getTransactionLogsDirectory();
        prepareDirectory( fs, dbDirectory );
        prepareDirectory( fs, txDirectory );

        fs.copyRecursively( source, dbDirectory );
        Path[] logFiles = fs.listFiles( dbDirectory, DEFAULT_FILENAME_FILTER );
        for ( Path logFile : logFiles )
        {
            fs.moveToDirectory( logFile, txDirectory );
        }
    }

    private static void prepareDirectory( FileSystemAbstraction fs, Path destination ) throws IOException
    {
        fs.deleteRecursively( destination );
        fs.mkdirs( destination );
    }

    static Path findFormatStoreDirectoryForVersion( String version, Path targetDir ) throws IOException
    {
        if ( StandardV4_3.STORE_VERSION.equals( version ) )
        {
            return find43FormatStoreDirectory( targetDir );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown version" );
        }
    }

    private static Path find43FormatStoreDirectory( Path targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy43Store.class, "upgradeTest43Db.zip", targetDir );
    }

    public static boolean checkNeoStoreHasFormatVersion( StoreVersionCheck check, RecordFormats expectedFormat )
    {
        String expectedVersion = expectedFormat.storeVersion();
        boolean successful = check.checkUpgrade( expectedVersion, CursorContext.NULL_CONTEXT ).outcome().isSuccessful();
        if ( successful )
        {
            String storeVersion = check.storeVersion( CursorContext.NULL_CONTEXT )
                    .orElseThrow( () -> new RuntimeException( "Expected store to have a store version." ) );
            return expectedVersion.equals( storeVersion );
        }
        return false;
    }

    public static void verifyFilesHaveSameContent( FileSystemAbstraction fileSystem, Path original, Path other )
            throws IOException
    {
        final int bufferBatchSize = 32 * 1024;
        Path[] files = fileSystem.listFiles( original );
        for ( Path originalFile : files )
        {
            Path otherFile = other.resolve( originalFile.getFileName() );
            if ( !fileSystem.isDirectory( originalFile ) )
            {
                try ( StoreChannel originalChannel = fileSystem.read( originalFile );
                      StoreChannel otherChannel = fileSystem.read( otherFile ) )
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

                        assertArrayEquals( originalBytes, otherBytes, "Different content in " + originalFile );
                    }
                }
            }
        }
    }

    private static boolean readAndFlip( ReadableByteChannel channel, ByteBuffer buffer, int bytes )
            throws IOException
    {
        buffer.clear();
        buffer.limit( bytes );
        while ( buffer.hasRemaining() )
        {
            int read = channel.read( buffer );

            if ( read == -1 )
            {
                return false;
            }
        }
        buffer.flip();
        return true;
    }
}
