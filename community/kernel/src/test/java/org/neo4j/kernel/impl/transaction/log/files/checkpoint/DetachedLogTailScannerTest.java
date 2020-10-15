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
import java.time.Instant;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_corrupted_log_files;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;

class DetachedLogTailScannerTest extends AbstractLogTailScannerTest
{
    @Override
    protected LogFiles createLogFiles() throws IOException
    {
        return LogFilesBuilder
                .activeFilesBuilder( databaseLayout, fs, pageCache )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( transactionIdStore )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .withLogProvider( logProvider )
                .withConfig( Config.defaults( fail_on_corrupted_log_files, false ) )
                .build();
    }

    @Override
    protected void writeCheckpoint( LogEntryWriter transactionLogWriter, CheckpointFile separateCheckpointFile, LogPosition logPosition ) throws IOException
    {
        separateCheckpointFile.getCheckpointAppender().checkPoint( LogCheckPointEvent.NULL, logPosition, Instant.now(), "test" );
    }
}
