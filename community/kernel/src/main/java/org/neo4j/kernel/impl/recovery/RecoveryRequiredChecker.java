/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.recovery;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.kernel.recovery.PositionToRecoverFrom;

/**
 * An external tool that can determine if a given store will need recovery.
 */
public class RecoveryRequiredChecker
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;

    public RecoveryRequiredChecker( FileSystemAbstraction fs, PageCache pageCache )
    {
        this.fs = fs;
        this.pageCache = pageCache;
    }

    public boolean isRecoveryRequiredAt( File dataDir ) throws IOException
    {
        File neoStore = new File( dataDir, MetaDataStore.DEFAULT_NAME );
        boolean noStoreFound = !NeoStores.isStorePresent( pageCache, dataDir );

        // We need config to determine where the logical log files are
        if ( noStoreFound )
        {
            // No database in the specified directory.
            return false;
        }

        long logVersion = MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.LOG_VERSION );
        PhysicalLogFiles logFiles = new PhysicalLogFiles( dataDir, fs );

        LogEntryReader<ReadableLogChannel> reader = new VersionAwareLogEntryReader<>( LogEntryVersion.CURRENT.byteCode() );

        LatestCheckPointFinder finder = new LatestCheckPointFinder( logFiles, fs, reader );
        return new PositionToRecoverFrom( finder ).apply( logVersion ) != LogPosition.UNSPECIFIED;
    }
}
