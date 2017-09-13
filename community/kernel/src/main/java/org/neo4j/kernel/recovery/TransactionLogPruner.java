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
package org.neo4j.kernel.recovery;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.function.LongConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;

import static java.lang.String.format;

/**
 * Transaction log pruner used during recovery to truncate all the logs after some specified position, that
 * recovery threats as corrupted or non-readable.
 * Transaction log file specified by provided log position will be truncated to provided length, any
 * subsequent files will be removed.
 * Any removed or modified log content will be stored in separate corruption logs archive for further analysis and as
 * an additional safety option to have the possibility to fully restore original logs in a faulty case.
 */
public class TransactionLogPruner
{
    static final String CORRUPTED_TX_LOGS_FOLDER_NAME = "corrupted-tx-logs";
    private static final String LOG_FILE_ARCHIVE_PATTERN = "corrupted-logs-%d-%d-%d.zip";

    private final File storeDir;
    private final PhysicalLogFiles logFiles;
    private final FileSystemAbstraction fs;

    public TransactionLogPruner( File storeDir, PhysicalLogFiles logFiles, FileSystemAbstraction fs )
    {
        this.storeDir = storeDir;
        this.logFiles = logFiles;
        this.fs = fs;
    }

    /**
     * Prune all transaction logs after provided position. Log version specified in a position will be
     * truncated to provided byte offset, any subsequent log files will be deleted. Backup copy of removed data will
     * be stored in separate archive.
     * @param positionAfterLastRecoveredTransaction position after last recovered transaction
     * @throws IOException
     */
    public void prune( LogPosition positionAfterLastRecoveredTransaction ) throws IOException
    {
        long recoveredTransactionLogVersion = positionAfterLastRecoveredTransaction.getLogVersion();
        long recoveredTransactionOffset = positionAfterLastRecoveredTransaction.getByteOffset();
        if ( isRecoveredLogCorrupted( recoveredTransactionLogVersion, recoveredTransactionOffset ) ||
                isNotLastLogFile( recoveredTransactionLogVersion ) )
        {
            backupCorruptedContent( recoveredTransactionLogVersion, recoveredTransactionOffset );
            truncateLogFiles( recoveredTransactionLogVersion, recoveredTransactionOffset );
        }
    }

    private void truncateLogFiles( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
            throws IOException
    {
        File lastRecoveredTransactionLog = logFiles.getLogFileForVersion( recoveredTransactionLogVersion );
        fs.truncate( lastRecoveredTransactionLog, recoveredTransactionOffset );
        forEachSubsequentLogFile( recoveredTransactionLogVersion,
                fileIndex -> fs.deleteFile( logFiles.getLogFileForVersion( fileIndex ) ) );
    }

    private void forEachSubsequentLogFile( long recoveredTransactionLogVersion, LongConsumer action )
    {
        long highestLogVersion = logFiles.getHighestLogVersion();
        for ( long fileIndex = recoveredTransactionLogVersion + 1; fileIndex <= highestLogVersion; fileIndex++ )
        {
            action.accept( fileIndex );
        }
    }

    private void backupCorruptedContent( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
            throws IOException
    {
        File corruptedLogArchive = getArchiveFile( recoveredTransactionLogVersion, recoveredTransactionOffset );
        try ( ZipOutputStream recoveryContent = new ZipOutputStream(
                new BufferedOutputStream( fs.openAsOutputStream( corruptedLogArchive, false ) ) ) )
        {
            ByteBuffer zipBuffer = ByteBuffer.allocate( (int) ByteUnit.mebiBytes( 1 ) );
            copyTransactionLogContent( recoveredTransactionLogVersion, recoveredTransactionOffset, recoveryContent,
                    zipBuffer );
            forEachSubsequentLogFile( recoveredTransactionLogVersion, fileIndex ->
            {
                try
                {
                    copyTransactionLogContent( fileIndex, 0, recoveryContent, zipBuffer );
                }
                catch ( IOException io )
                {
                    throw new UncheckedIOException( io );
                }
            } );
        }
    }

    private File getArchiveFile( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
    {
        File corruptedLogsFolder = new File( storeDir, CORRUPTED_TX_LOGS_FOLDER_NAME );
        assert fs.mkdir( corruptedLogsFolder );
        return new File( corruptedLogsFolder,
                format( LOG_FILE_ARCHIVE_PATTERN, recoveredTransactionLogVersion, recoveredTransactionOffset,
                        System.currentTimeMillis() ) );
    }

    private void copyTransactionLogContent( long logFileIndex, long logOffset, ZipOutputStream destination,
            ByteBuffer byteBuffer ) throws IOException
    {
        File logFile = logFiles.getLogFileForVersion( logFileIndex );
        ZipEntry zipEntry = new ZipEntry( logFile.getName() );
        destination.putNextEntry( zipEntry );
        try ( StoreChannel transactionLogChannel = fs.open( logFile, "r" ) )
        {
            transactionLogChannel.position( logOffset );
            while ( transactionLogChannel.read( byteBuffer ) >= 0 )
            {
                byteBuffer.flip();
                destination.write( byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining() );
                byteBuffer.clear();
            }
        }
        destination.closeEntry();
    }

    private boolean isNotLastLogFile( long recoveredTransactionLogVersion )
    {
        return logFiles.getHighestLogVersion() > recoveredTransactionLogVersion;
    }

    private boolean isRecoveredLogCorrupted( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
    {
        return logFiles.getLogFileForVersion( recoveredTransactionLogVersion ).length() > recoveredTransactionOffset;
    }
}
