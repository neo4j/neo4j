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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CorruptedLogsTruncatorTest
{
    private static final int SINGLE_LOG_FILE_SIZE = 25;
    private static final int TOTAL_NUMBER_OF_LOG_FILES = 12;
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final LifeRule life = new LifeRule();
    private File storeDir;
    private LogFiles logFiles;
    private CorruptedLogsTruncator logPruner;

    @Before
    public void setUp() throws Exception
    {
        storeDir = testDirectory.graphDbDir();
        SimpleLogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDir, fileSystemRule )
                .withRotationThreshold( LogHeader.LOG_HEADER_SIZE + 9L )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore ).build();
        life.add( logFiles );
        logPruner = new CorruptedLogsTruncator( storeDir, logFiles, fileSystemRule );
    }

    @Test
    public void doNotPruneEmptyLogs() throws IOException
    {
        logPruner.truncate( LogPosition.start( 0 ) );
        assertTrue( FileUtils.isEmptyDirectory( storeDir ) );
    }

    @Test
    public void doNotPruneNonCorruptedLogs() throws IOException
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
        assertTrue( ArrayUtil.isEmpty( storeDir.listFiles( File::isDirectory ) ) );
    }

    @Test
    public void pruneAndArchiveLastLog() throws IOException
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

        File corruptedLogsDirectory = new File( storeDir, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( corruptedLogsDirectory.exists() );
        File[] files = corruptedLogsDirectory.listFiles();
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
    public void pruneAndArchiveMultipleLogs() throws IOException
    {
        life.start();
        generateTransactionLogFiles( logFiles );

        long highestCorrectLogFileIndex = 5;
        File highestCorrectLogFile = logFiles.getLogFileForVersion( highestCorrectLogFileIndex );
        long fileSizeBeforePrune = highestCorrectLogFile.length();
        int bytesToPrune = 7;
        long byteOffset = fileSizeBeforePrune - bytesToPrune;
        LogPosition prunePosition = new LogPosition( highestCorrectLogFileIndex, byteOffset );
        life.shutdown();

        logPruner.truncate( prunePosition );

        life.start();
        assertEquals( 6, logFiles.logFiles().length );
        assertEquals( byteOffset, highestCorrectLogFile.length() );

        File corruptedLogsDirectory = new File( storeDir, CorruptedLogsTruncator.CORRUPTED_TX_LOGS_BASE_NAME );
        assertTrue( corruptedLogsDirectory.exists() );
        File[] files = corruptedLogsDirectory.listFiles();
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
                checkEntryNameAndSize( zipFile, TransactionLogFiles.DEFAULT_NAME + "." + index, SINGLE_LOG_FILE_SIZE );
            }
            checkEntryNameAndSize( zipFile, TransactionLogFiles.DEFAULT_NAME + "." + lastFileIndex,
                    SINGLE_LOG_FILE_SIZE - 1 );
        }
    }

    private void checkEntryNameAndSize( ZipFile zipFile, String entryName, int expectedSize ) throws IOException
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
