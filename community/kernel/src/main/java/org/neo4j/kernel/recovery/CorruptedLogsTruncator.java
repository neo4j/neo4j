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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.function.LongConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.memory.MemoryTracker;

import static java.lang.String.format;
import static org.neo4j.io.ByteUnit.MebiByte;

/**
 * Transaction log truncator used during recovery to truncate all the logs after some specified position, that
 * recovery treats as corrupted or non-readable.
 * Transaction log file specified by provided log position will be truncated to provided length, any
 * subsequent files will be removed.
 * Any removed or modified log content will be stored in separate corruption logs archive for further analysis and as
 * an additional safety option to have the possibility to fully restore original logs in a faulty case.
 */
public class CorruptedLogsTruncator
{
    public static final String CORRUPTED_TX_LOGS_BASE_NAME = "corrupted-" + TransactionLogFilesHelper.DEFAULT_NAME;
    private static final String LOG_FILE_ARCHIVE_PATTERN = CORRUPTED_TX_LOGS_BASE_NAME + "-%d-%d-%d.zip";

    private final Path storeDir;
    private final LogFiles logFiles;
    private final FileSystemAbstraction fs;
    private final MemoryTracker memoryTracker;

    public CorruptedLogsTruncator( Path storeDir, LogFiles logFiles, FileSystemAbstraction fs, MemoryTracker memoryTracker )
    {
        this.storeDir = storeDir;
        this.logFiles = logFiles;
        this.fs = fs;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Truncate all transaction logs after provided position. Log version specified in a position will be
     * truncated to provided byte offset, any subsequent log files will be deleted. Backup copy of removed data will
     * be stored in separate archive.
     * @param positionAfterLastRecoveredTransaction position after last recovered transaction
     * @throws IOException
     */
    public void truncate( LogPosition positionAfterLastRecoveredTransaction ) throws IOException
    {
        long recoveredTransactionLogVersion = positionAfterLastRecoveredTransaction.getLogVersion();
        long recoveredTransactionOffset = positionAfterLastRecoveredTransaction.getByteOffset();
        if ( isRecoveredLogCorrupted( recoveredTransactionLogVersion, recoveredTransactionOffset ) ||
                haveMoreRecentLogFiles( recoveredTransactionLogVersion ) )
        {
            backupCorruptedContent( recoveredTransactionLogVersion, recoveredTransactionOffset );
            truncateLogFiles( recoveredTransactionLogVersion, recoveredTransactionOffset );
        }
    }

    private void truncateLogFiles( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
            throws IOException
    {
        var logFile = logFiles.getLogFile();
        Path lastRecoveredTransactionLog = logFile.getLogFileForVersion( recoveredTransactionLogVersion );
        fs.truncate( lastRecoveredTransactionLog.toFile(), recoveredTransactionOffset );
        forEachSubsequentLogFile( recoveredTransactionLogVersion,
                fileIndex -> fs.deleteFile( logFile.getLogFileForVersion( fileIndex ).toFile() ) );
    }

    private void forEachSubsequentLogFile( long recoveredTransactionLogVersion, LongConsumer action )
    {
        long highestLogVersion = logFiles.getLogFile().getHighestLogVersion();
        for ( long fileIndex = recoveredTransactionLogVersion + 1; fileIndex <= highestLogVersion; fileIndex++ )
        {
            action.accept( fileIndex );
        }
    }

    private void backupCorruptedContent( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
            throws IOException
    {
        Path corruptedLogArchive = getArchiveFile( recoveredTransactionLogVersion, recoveredTransactionOffset );
        try ( ZipOutputStream recoveryContent = new ZipOutputStream(
                fs.openAsOutputStream( corruptedLogArchive.toFile(), false ) );
                var bufferScope = new HeapScopedBuffer(  1, MebiByte, memoryTracker ) )
        {
            copyTransactionLogContent( recoveredTransactionLogVersion, recoveredTransactionOffset, recoveryContent, bufferScope.getBuffer() );
            forEachSubsequentLogFile( recoveredTransactionLogVersion, fileIndex ->
            {
                try
                {
                    copyTransactionLogContent( fileIndex, 0, recoveryContent, bufferScope.getBuffer() );
                }
                catch ( IOException io )
                {
                    throw new UncheckedIOException( io );
                }
            } );
        }
    }

    private Path getArchiveFile( long recoveredTransactionLogVersion, long recoveredTransactionOffset )
            throws IOException
    {
        Path corruptedLogsFolder = storeDir.resolve( CORRUPTED_TX_LOGS_BASE_NAME );
        fs.mkdirs( corruptedLogsFolder.toFile() );
        return corruptedLogsFolder.resolve(
                format( LOG_FILE_ARCHIVE_PATTERN, recoveredTransactionLogVersion, recoveredTransactionOffset,
                        System.currentTimeMillis() ) );
    }

    private void copyTransactionLogContent( long logFileIndex, long logOffset, ZipOutputStream destination,
            ByteBuffer byteBuffer ) throws IOException
    {
        Path logFile = logFiles.getLogFile().getLogFileForVersion( logFileIndex );
        if ( fs.getFileSize( logFile.toFile() ) == logOffset )
        {
            // file was recovered fully, nothing to backup
            return;
        }
        ZipEntry zipEntry = new ZipEntry( logFile.getFileName().toString() );
        destination.putNextEntry( zipEntry );
        try ( StoreChannel transactionLogChannel = fs.read( logFile.toFile() ) )
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

    private boolean haveMoreRecentLogFiles( long recoveredTransactionLogVersion )
    {
        return logFiles.getLogFile().getHighestLogVersion() > recoveredTransactionLogVersion;
    }

    private boolean isRecoveredLogCorrupted( long recoveredTransactionLogVersion, long recoveredTransactionOffset ) throws IOException
    {
        try
        {
            return Files.size( logFiles.getLogFile().getLogFileForVersion( recoveredTransactionLogVersion ) ) > recoveredTransactionOffset;
        }
        catch ( NoSuchFileException ignored )
        {
            return false;
        }
    }
}
