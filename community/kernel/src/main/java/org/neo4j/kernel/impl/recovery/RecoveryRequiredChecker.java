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
package org.neo4j.kernel.impl.recovery;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.kernel.recovery.RecoveryStartInformationProvider;

import static org.neo4j.kernel.recovery.RecoveryStartInformationProvider.NO_MONITOR;

/**
 * An external tool that can determine if a given store will need recovery.
 */
public class RecoveryRequiredChecker
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Monitors monitors;
    private Config config;

    public RecoveryRequiredChecker( FileSystemAbstraction fs, PageCache pageCache, Config config, Monitors monitors )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.config = config;
        this.monitors = monitors;
    }

    public boolean isRecoveryRequiredAt( File dataDir ) throws IOException
    {
        // We need config to determine where the logical log files are
        if ( !NeoStores.isStorePresent( pageCache, dataDir ) )
        {
            return false;
        }

        LogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( dataDir, fs, pageCache )
                                           .withConfig( config )
                                           .withLogEntryReader( reader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, reader, monitors );
        return new RecoveryStartInformationProvider( tailScanner, NO_MONITOR ).get().isRecoveryRequired();
    }
}
