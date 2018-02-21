/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.tools.dump.TransactionLogAnalyzer;

public class BackupTransactionLogFilesHelper
{
    LogFiles readLogFiles( File backupDir ) throws IOException
    {
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction );
        return LogFilesBuilder.activeFilesBuilder( backupDir, fileSystemAbstraction, pageCache ).build();
    }

    Pair<List<LogEntry[]>,List<CheckPoint>> logEntriesAndCheckpoints( File logFile )
    {
        TransactionLogAnalyzer transactionLogAnalyzer = new TransactionLogAnalyzer();
        List<LogEntry[]> listOfTransactions = new ArrayList<>();
        List<CheckPoint> listOfCheckpoints = new ArrayList<>();
        TransactionLogAnalyzer.Monitor monitor = new TransactionLogAnalyzer.Monitor()
        {
            @Override
            public void transaction( LogEntry[] transactionEntries )
            {
                listOfTransactions.add( transactionEntries );
            }

            @Override
            public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
            {
                listOfCheckpoints.add( checkpoint );
            }
        };
        InvalidLogEntryHandler invalidLogEntryHandler = new InvalidLogEntryHandler()
        {
            @Override
            public boolean handleInvalidEntry( Exception e, LogPosition position )
            {
                throw new RuntimeException( position.toString(), e );
            }
        };
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        try
        {
            transactionLogAnalyzer.analyze( fileSystemAbstraction, logFile, invalidLogEntryHandler, monitor );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return Pair.of( listOfTransactions, listOfCheckpoints );
    }
}
