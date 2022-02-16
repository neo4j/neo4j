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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.kernel.impl.transaction.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryDetachedCheckpointV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

public class CheckpointInfoFactory
{
    public static CheckpointInfo ofLogEntry( LogEntry entry, LogPosition checkpointEntryPosition, LogPosition channelPositionAfterCheckpoint,
            LogPosition checkpointFilePostReadPosition, TransactionLogFilesContext context, LogFile logFile )
    {
        if ( entry instanceof LogEntryDetachedCheckpointV4_2 checkpoint42 )
        {
            return new CheckpointInfo( checkpoint42.getLogPosition(), checkpoint42.getStoreId(), checkpointEntryPosition, channelPositionAfterCheckpoint,
                    checkpointFilePostReadPosition, checkpoint42.getVersion(), readTransactionId( context, logFile, checkpoint42.getLogPosition() ),
                    checkpoint42.getReason() );
        }
        else if ( entry instanceof LogEntryDetachedCheckpointV5_0 checkpoint50 )
        {
            return new CheckpointInfo( checkpoint50.getLogPosition(), checkpoint50.getStoreId(), checkpointEntryPosition, channelPositionAfterCheckpoint,
                    checkpointFilePostReadPosition, checkpoint50.getVersion(), checkpoint50.getTransactionId(), checkpoint50.getReason() );
        }
        else
        {
            throw new UnsupportedOperationException( "Expected to observe only checkpoint entries, but: `" + entry + "` was found." );
        }
    }

    public static TransactionId readTransactionId( TransactionLogFilesContext context, LogFile logFile, LogPosition transactionPosition )
    {
        try ( var channel = logFile.openForVersion( transactionPosition.getLogVersion() );
              var reader = new ReadAheadLogChannel( new UnclosableChannel( channel ), NO_MORE_CHANNELS, context.getMemoryTracker() );
              var logEntryCursor = new LogEntryCursor( new VersionAwareLogEntryReader( context.getCommandReaderFactory() ), reader ) )
        {
            LogPosition checkedPosition = null;
            while ( logEntryCursor.next() )
            {
                LogEntry logEntry = logEntryCursor.get();
                checkedPosition = reader.getCurrentPosition();
                if ( logEntry instanceof LogEntryCommit commit && checkedPosition.equals( transactionPosition ) )
                {
                    return new TransactionId( commit.getTxId(), commit.getChecksum(), commit.getTimeWritten() );
                }
            }
            // We have a checkpoint on this point but there is no transaction found that match it and log files are corrupted.
            // Database should be restored from last the last valid backup or dump in normal circumstances.
            if ( !context.getConfig().get( fail_on_corrupted_log_files ) )
            {
                return TransactionIdStore.UNKNOWN_TRANSACTION_ID;
            }
            throw new IllegalStateException(
                    "Checkpoint record pointed to " + transactionPosition + ", but log commit entry not found at that position. " + "Last checked position: " +
                            checkedPosition );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Unable to find last transaction id log files. Position: " + transactionPosition, e );
        }
    }
}
