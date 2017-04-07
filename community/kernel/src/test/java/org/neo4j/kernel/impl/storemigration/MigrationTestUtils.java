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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v30.Legacy30Store;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.string.UTF8;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.readAndFlip;

public class MigrationTestUtils
{
    private MigrationTestUtils()
    {
    }

    public static int[] makeLongArray()
    {
        int[] longArray = new int[100];
        for ( int i = 0; i < 100; i++ )
        {
            longArray[i] = i;
        }
        return longArray;
    }

    public static String makeLongString()
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < 100; i++ )
        {
            builder.append( "characters" );
        }
        return builder.toString();
    }

    static void changeVersionNumber( FileSystemAbstraction fileSystem, File storeFile, String versionString )
            throws IOException
    {
        byte[] versionBytes = UTF8.encode( versionString );
        try ( StoreChannel fileChannel = fileSystem.open( storeFile, "rw" ) )
        {
            fileChannel.position( fileSystem.getFileSize( storeFile ) - versionBytes.length );
            fileChannel.write( ByteBuffer.wrap( versionBytes ) );
        }
    }

    public static void prepareSampleLegacyDatabase( String version, EphemeralFileSystemAbstraction workingFs,
            File workingDirectory, File realDirForPreparingDatabase ) throws IOException
    {
        try ( DefaultFileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction() )
        {
            File resourceDirectory = findFormatStoreDirectoryForVersion( version, realDirForPreparingDatabase );
            workingFs.copyRecursivelyFromOtherFs( resourceDirectory, fileSystemAbstraction,
                    workingDirectory );
        }
    }

    public static void prepareSampleLegacyDatabase( String version, FileSystemAbstraction workingFs,
            File workingDirectory, File prepareDirectory ) throws IOException
    {
        if ( !prepareDirectory.exists() )
        {
            throw new IllegalArgumentException( "bad prepare directory" );
        }
        File resourceDirectory = findFormatStoreDirectoryForVersion( version, prepareDirectory );
        workingFs.deleteRecursively( workingDirectory );
        workingFs.mkdirs( workingDirectory );
        workingFs.copyRecursively( resourceDirectory, workingDirectory );
    }

    static File findFormatStoreDirectoryForVersion( String version, File targetDir ) throws IOException
    {
        if ( StandardV2_3.STORE_VERSION.equals( version ) )
        {
            return find23FormatStoreDirectory( targetDir );
        }
        else if ( StandardV3_0.STORE_VERSION.equals( version ) )
        {
            return find30FormatStoreDirectory( targetDir );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown version" );
        }
    }

    private static File find30FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy30Store.class, "upgradeTest30Db.zip", targetDir );
    }

    public static File find23FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy23Store.class, "upgradeTest23Db.zip", targetDir );
    }

    public static boolean checkNeoStoreHasDefaultFormatVersion( StoreVersionCheck check, File workingDirectory )
    {
        File neostoreFile = new File( workingDirectory, MetaDataStore.DEFAULT_NAME );
        return check.hasVersion( neostoreFile, RecordFormatSelector.defaultFormat().storeVersion() )
                .outcome.isSuccessful();
    }

    public static void verifyFilesHaveSameContent( FileSystemAbstraction fileSystem, File original, File other )
            throws IOException
    {
        final int bufferBatchSize = 32 * 1024;
        File[] files = fileSystem.listFiles( original );
        for ( File originalFile : files )
        {
            File otherFile = new File( other, originalFile.getName() );
            if ( !fileSystem.isDirectory( originalFile ) )
            {
                try ( StoreChannel originalChannel = fileSystem.open( originalFile, "r" );
                      StoreChannel otherChannel = fileSystem.open( otherFile, "r" ) )
                {
                    ByteBuffer buffer = ByteBuffer.allocate( bufferBatchSize );
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

    public static void removeCheckPointFromTxLog( FileSystemAbstraction fileSystem, File workingDirectory )
            throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( workingDirectory, fileSystem );
        LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fileSystem, logEntryReader );
        LatestCheckPointFinder.LatestCheckPoint latestCheckPoint = finder.find( logFiles.getHighestLogVersion() );

        if ( latestCheckPoint.commitsAfterCheckPoint )
        {
            // done already
            return;
        }

        // let's assume there is at least a checkpoint
        assertNotNull( latestCheckPoint.checkPoint );

        LogPosition logPosition = latestCheckPoint.checkPoint.getLogPosition();
        File logFile = logFiles.getLogFileForVersion( logPosition.getLogVersion() );
        fileSystem.truncate( logFile, logPosition.getByteOffset() );
    }
}
