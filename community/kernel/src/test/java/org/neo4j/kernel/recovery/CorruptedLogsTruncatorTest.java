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
package org.neo4j.kernel.recovery;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;

@TestDirectoryExtension
class CorruptedLogsTruncatorTest
{
    private static final long SINGLE_LOG_FILE_SIZE = LogHeader.LOG_HEADER_SIZE + 9L;
    private static final int TOTAL_NUMBER_OF_LOG_FILES = 12;

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private final LifeSupport life = new LifeSupport();

    private File databaseDirectory;
    private LogFiles logFiles;
    private CorruptedLogsTruncator logPruner;

    @BeforeEach
    void setUp() throws Exception
    {
        databaseDirectory = testDirectory.storeDir();
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseDirectory, fs )
                .withRotationThreshold( SINGLE_LOG_FILE_SIZE )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withLogEntryReader( logEntryReader() )
                .build();
        life.add( logFiles );
        logPruner = new CorruptedLogsTruncator( databaseDirectory, logFiles, fs );
    }

    @AfterEach
    void tearDown()
    {
        life.shutdown();
    }

    @Test
    void doNotPruneEmptyLogs() throws IOException
    {
        logPruner.truncate( LogPosition.start( 0 ) );
        assertTrue( FileSystemUtils.isEmptyOrNonExistingDirectory( fs, databaseDirectory ) );
    }

    @Test
    void doNotPruneNonCorruptedLogs() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        long highestLogVersion = logFiles.getHighestLogVersion();
        long fileSizeBeforePrune = logFiles.getHighestLogFile().length();
        LogPosition endOfLogsPosition = new LogPosition( highestLogVersion, fileSizeBeforePrune );
        assertEquals( TOTAL_NUMBER_OF_LOG_FILES - 1, highestLogVersion );

        logPruner.truncate( endOfLogsPosition );

        assertEquals( TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length );
        assertEquals( fileSizeBeforePrune, logFiles.getHighestLogFile().length() );
        assertTrue( ArrayUtils.isEmpty( databaseDirectory.listFiles( File::isDirectory ) ) );
    }

    @Test
    void pruneAndArchiveLastLog() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        long highestLogVersion = logFiles.getHighestLogVersion();
        File highestLogFile = logFiles.getHighestLogFile();
        long fileSizeBeforePrune = highestLogFile.length();
        int bytesToPrune = 5;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition( highestLogVersion, byteOffset );

        logPruner.truncate( prunePosition );

        assertEquals( TOTAL_NUMBER_OF_LOG_FILES, logFiles.logFiles().length );
        assertEquals( byteOffset, highestLogFile.length() );

        File corruptedLogsDirectory = new File( databaseDirectory, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( corruptedLogsDirectory.exists() );
        File[] files = corruptedLogsDirectory.listFiles();
        assertNotNull( files );
        assertEquals( 1, files.length );

        File corruptedLogsArchive = files[0];
        checkArchiveName( highestLogVersion, byteOffset, corruptedLogsArchive );
        try ( ZipFile zipFile = new ZipFile( corruptedLogsArchive ) )
        {
            assertEquals( 1, zipFile.size() );
            checkEntryNameAndSize( zipFile, highestLogFile.getName(), bytesToPrune );
        }
    }

    @Test
    void pruneAndArchiveMultipleLogs() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        long highestCorrectLogFileIndex = 5;
        File highestCorrectLogFile = logFiles.getLogFileForVersion( highestCorrectLogFileIndex );
        long fileSizeBeforePrune = highestCorrectLogFile.length();
        long highestLogFileLength = logFiles.getHighestLogFile().length();
        int bytesToPrune = 7;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition( highestCorrectLogFileIndex, byteOffset );
        life.shutdown();

        logPruner.truncate( prunePosition );

        life.start();
        assertEquals( 6, logFiles.logFiles().length );
        assertEquals( byteOffset, highestCorrectLogFile.length() );

        File corruptedLogsDirectory = new File( databaseDirectory, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( corruptedLogsDirectory.exists() );
        File[] files = corruptedLogsDirectory.listFiles();
        assertNotNull( files );
        assertEquals( 1, files.length );

        File corruptedLogsArchive = files[0];
        checkArchiveName( highestCorrectLogFileIndex, byteOffset, corruptedLogsArchive );
        try ( ZipFile zipFile = new ZipFile( corruptedLogsArchive ) )
        {
            assertEquals( 7, zipFile.size() );
            checkEntryNameAndSize( zipFile, highestCorrectLogFile.getName(), bytesToPrune );
            long nextLogFileIndex = highestCorrectLogFileIndex + 1;
            int lastFileIndex = TOTAL_NUMBER_OF_LOG_FILES - 1;
            for ( long index = nextLogFileIndex; index < lastFileIndex; index++ )
            {
                checkEntryNameAndSize( zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + index, SINGLE_LOG_FILE_SIZE );
            }
            checkEntryNameAndSize( zipFile, TransactionLogFilesHelper.DEFAULT_NAME + "." + lastFileIndex, highestLogFileLength );
        }
    }

    private void checkEntryNameAndSize( ZipFile zipFile, String entryName, long expectedSize ) throws IOException
    {
        ZipEntry entry = zipFile.getEntry( entryName );
        InputStream inputStream = zipFile.getInputStream( entry );
        int entryBytes = 0;
        while ( inputStream.read() >= 0 )
        {
            entryBytes++;
        }
        assertEquals( expectedSize, entryBytes );
    }

    private void checkArchiveName( long highestLogVersion, long byteOffset, File corruptedLogsArchive )
    {
        String name = corruptedLogsArchive.getName();
        assertTrue( name.startsWith( "corrupted-neostore.transaction.db-" + highestLogVersion + "-" + byteOffset ) );
        assertTrue( FilenameUtils.isExtension( name, "zip" ) );
    }

    private void generateTransactionLogFiles( LogFiles logFiles ) throws IOException
    {
        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChannel writer = logFile.getWriter();
        for ( byte i = 0; i < 107; i++ )
        {
            writer.put( i );
            writer.prepareForFlush();
            if ( logFile.rotationNeeded() )
            {
                logFile.rotate();
            }
        }
    }
}
