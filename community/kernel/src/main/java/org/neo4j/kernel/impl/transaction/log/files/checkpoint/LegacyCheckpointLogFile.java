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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;

import static java.util.Collections.emptyList;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

public class LegacyCheckpointLogFile
{
    private final InlinedLogTailScanner logTailScanner;
    private final TransactionLogFiles logFiles;
    private final TransactionLogFilesContext context;

    public LegacyCheckpointLogFile( TransactionLogFiles logFiles, TransactionLogFilesContext context )
    {
        this.logFiles = logFiles;
        this.context = context;
        this.logTailScanner = new InlinedLogTailScanner( logFiles, context );
    }

    public Optional<CheckpointInfo> findLatestCheckpoint()
    {
        return Optional.ofNullable( getTailInformation().lastCheckPoint );
    }

    public List<CheckpointInfo> reachableCheckpoints() throws IOException
    {
        var logFile = logFiles.getLogFile();
        long highestVersion = logFile.getHighestLogVersion();
        if ( highestVersion < 0 )
        {
            return emptyList();
        }

        long lowestVersion = logFile.getLowestLogVersion();
        long currentVersion = highestVersion;

        var checkpoints = new ArrayList<CheckpointInfo>();
        while ( currentVersion >= lowestVersion )
        {
            try ( var channel = logFile.openForVersion( currentVersion );
                    var reader = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, context.getMemoryTracker() );
                    var logEntryCursor = new LogEntryCursor( context.getLogEntryReader(), reader ) )
            {
                LogHeader logHeader = logFile.extractHeader( currentVersion );
                var storeId = logHeader.getStoreId();
                LogPosition lastLocation = reader.getCurrentPosition();
                while ( logEntryCursor.next() )
                {
                    LogEntry logEntry = logEntryCursor.get();
                    // Collect data about latest checkpoint
                    if ( logEntry instanceof LogEntryInlinedCheckPoint )
                    {
                        checkpoints.add( new CheckpointInfo( (LogEntryInlinedCheckPoint) logEntry, storeId, lastLocation ) );
                    }
                    lastLocation = reader.getCurrentPosition();
                }
                currentVersion--;
            }
        }
        return checkpoints;
    }

    public LogTailInformation getTailInformation()
    {
        return logTailScanner.getTailInformation();
    }
}
