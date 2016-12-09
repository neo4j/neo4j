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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Predicate;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0_7;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
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

    public static void changeVersionNumber( FileSystemAbstraction fileSystem, File storeFile, String versionString )
            throws IOException
    {
        byte[] versionBytes = UTF8.encode( versionString );
        try( StoreChannel fileChannel = fileSystem.open( storeFile, "rw" ) )
        {
            fileChannel.position( fileSystem.getFileSize( storeFile ) - versionBytes.length );
            fileChannel.write( ByteBuffer.wrap( versionBytes ) );
        }
    }

    /**
     * Removes the version trailer from the store files.
     */
    public static void truncateFile( FileSystemAbstraction fileSystem, File storeFile,
            String suffixToDetermineTruncationLength ) throws IOException
    {
        byte[] versionBytes = UTF8.encode( suffixToDetermineTruncationLength );
        if ( !fileSystem.fileExists( storeFile ) )
        {
            return;
        }
        try ( StoreChannel fileChannel = fileSystem.open( storeFile, "rw" ) )
        {
            long fileSize = fileSystem.getFileSize( storeFile );
            fileChannel.truncate( Math.max( 0, fileSize - versionBytes.length ) );
        }
    }

    public static void truncateAllFiles( FileSystemAbstraction fileSystem, File workingDirectory,
                                         String version ) throws IOException
    {
        for ( StoreFile storeFile : StoreFile.legacyStoreFilesForVersion( version ) )
        {
            File file = new File( workingDirectory, storeFile.storeFileName() );
            truncateFile( fileSystem, file, storeFile.forVersion( version ) );
        }
    }

    public static void truncateToFixedLength( FileSystemAbstraction fileSystem, File storeFile, int newLength )
            throws IOException
    {
        try ( StoreChannel fileChannel = fileSystem.open( storeFile, "rw" ) )
        {
            fileChannel.truncate( newLength );
        }
    }

    public static void prepareSampleLegacyDatabase( String version, EphemeralFileSystemAbstraction workingFs,
            File workingDirectory, File realDirForPreparingDatabase ) throws IOException
    {
        File resourceDirectory = findFormatStoreDirectoryForVersion( version, realDirForPreparingDatabase );
        workingFs.copyRecursivelyFromOtherFs( resourceDirectory, new DefaultFileSystemAbstraction(), workingDirectory );
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

    public static File findFormatStoreDirectoryForVersion( String version, File targetDir ) throws IOException
    {
        if ( version.equals( StandardV3_0.STORE_VERSION ) )
        {
            return find30FormatStoreDirectory( targetDir );
        }
        else if ( version.equals( StandardV2_3.STORE_VERSION ) )
        {
            return find23FormatStoreDirectory( targetDir );
        }
        else if ( version.equals( StandardV2_2.STORE_VERSION ) )
        {
            return find22FormatStoreDirectory( targetDir );
        }
        else if ( version.equals( StandardV2_1.STORE_VERSION ) )
        {
            return find21FormatStoreDirectory( targetDir );
        }
        else if ( version.equals( StandardV2_0.STORE_VERSION ) )
        {
            return find20FormatStoreDirectory( targetDir );
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

    private static File find23FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy23Store.class, "upgradeTest23Db.zip", targetDir );
    }

    public static File find22FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy22Store.class, "upgradeTest22Db.zip", targetDir );
    }

    public static File find21FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy21Store.class, "upgradeTest21Db.zip", targetDir );
    }

    public static File find21FormatStoreDirectoryWithDuplicateProperties( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy21Store.class, "with-duplicate-properties.zip", targetDir );
    }

    public static File find20FormatStoreDirectory( File targetDir ) throws IOException
    {
        return Unzip.unzip( Legacy20Store.class, "exampledb.zip", targetDir );
    }

    public static boolean allLegacyStoreFilesHaveVersion( FileSystemAbstraction fs, File dir, String version )
    {
        final Iterable<StoreFile> storeFilesWithGivenVersions =
                Iterables.filter( ALL_EXCEPT_COUNTS_STORE, StoreFile.legacyStoreFilesForVersion( version ) );
        LegacyStoreVersionCheck legacyStoreVersionCheck = new LegacyStoreVersionCheck( fs );
        boolean success = true;
        for ( StoreFile storeFile : storeFilesWithGivenVersions )
        {
            File file = new File( dir, storeFile.storeFileName() );
            success &=
                    legacyStoreVersionCheck.hasVersion( file, version, storeFile.isOptional() ).outcome.isSuccessful();
        }
        return success;
    }

    public static boolean allStoreFilesHaveNoTrailer( FileSystemAbstraction fs, File dir )
    {
        final Iterable<StoreFile> storeFilesWithGivenVersions =
                Iterables.filter( ALL_EXCEPT_COUNTS_STORE, StoreFile.legacyStoreFilesForVersion( StandardV3_0_7.STORE_VERSION ) );
        LegacyStoreVersionCheck legacyStoreVersionCheck = new LegacyStoreVersionCheck( fs );

        boolean success = true;
        for ( StoreFile storeFile : storeFilesWithGivenVersions )
        {
            File file = new File( dir, storeFile.storeFileName() );
            StoreVersionCheck.Result result =
                    legacyStoreVersionCheck.hasVersion( file, StandardV3_0_7.STORE_VERSION, storeFile.isOptional() );
            success &= result.outcome == Outcome.unexpectedStoreVersion ||
                       result.outcome == Outcome.storeVersionNotFound;
        }
        return success;
    }

    public static boolean containsAnyStoreFiles( FileSystemAbstraction fileSystem, File directory )
    {
        for ( StoreFile file : StoreFile.values() )
        {
            if ( fileSystem.fileExists( new File( directory, file.storeFileName() ) ) )
            {
                return true;
            }
        }
        return false;
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

    public static File isolatedMigrationDirectoryOf( File dbDirectory )
    {
        return new File( dbDirectory, "upgrade" );
    }

    private static final Predicate<StoreFile> ALL_EXCEPT_COUNTS_STORE =
            item -> item != StoreFile.COUNTS_STORE_LEFT && item != StoreFile.COUNTS_STORE_RIGHT;

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
