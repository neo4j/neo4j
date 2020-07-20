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

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RecoveryState;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;

import static org.neo4j.kernel.recovery.RecoveryStartInformationProvider.NO_MONITOR;

/**
 * Utility that can determine if a given store will need recovery.
 */
class RecoveryRequiredChecker
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Config config;
    private final StorageEngineFactory storageEngineFactory;

    RecoveryRequiredChecker( FileSystemAbstraction fs, PageCache pageCache, Config config, StorageEngineFactory storageEngineFactory )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
    }

    public boolean isRecoveryRequiredAt( DatabaseLayout databaseLayout, MemoryTracker memoryTracker ) throws IOException
    {
        LogEntryReader reader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        LogFiles logFiles = buildLogFiles( databaseLayout, reader, memoryTracker );
        return isRecoveryRequiredAt( databaseLayout, logFiles );
    }

    private LogFiles buildLogFiles( DatabaseLayout databaseLayout, LogEntryReader reader, MemoryTracker memoryTracker ) throws IOException
    {
        return LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache )
                    .withConfig( config )
                    .withMemoryTracker( memoryTracker )
                    .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                    .withLogEntryReader( reader ).build();
    }

    boolean isRecoveryRequiredAt( DatabaseLayout databaseLayout, LogFiles logFiles )
    {
        if ( !storageEngineFactory.storageExists( fs, databaseLayout, pageCache ) )
        {
            return false;
        }
        StorageFilesState filesRecoveryState = storageEngineFactory.checkRecoveryRequired( fs, databaseLayout, pageCache );
        if ( filesRecoveryState.getRecoveryState() != RecoveryState.RECOVERED )
        {
            return true;
        }
        return new RecoveryStartInformationProvider( logFiles, NO_MONITOR ).get().isRecoveryRequired();
    }
}
