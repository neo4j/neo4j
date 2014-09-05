/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacylogs;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.allLegacyLogFilesFilter;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.getLegacyLogVersion;
import static org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogFilenames.versionedLegacyLogFilesFilter;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_NAME;
import static org.neo4j.kernel.impl.transaction.log.PhysicalLogFile.DEFAULT_VERSION_SUFFIX;

public class LegacyLogs
{
    private final FileSystemAbstraction fs;
    private final LegacyLogEntryReader reader;
    private final LegacyLogEntryWriter writer;

    public LegacyLogs( FileSystemAbstraction fs )
    {
        this( fs, new LegacyLogEntryReader( fs ), new LegacyLogEntryWriter( fs ) );
    }

    LegacyLogs( FileSystemAbstraction fs, LegacyLogEntryReader reader, LegacyLogEntryWriter writer )
    {
        this.fs = fs;
        this.reader = reader;
        this.writer = writer;
    }

    public void migrateLogs( File storeDir, File migrationDir ) throws IOException
    {
        File[] logFiles = fs.listFiles( storeDir, versionedLegacyLogFilesFilter );

        for ( File file : logFiles )
        {
            final Pair<LogHeader, IOCursor<LogEntry>> pair = reader.openReadableChannel( file );
            final LogHeader header = pair.first();

            try ( IOCursor<LogEntry> cursor = pair.other();
                  LogVersionedStoreChannel channel =
                          writer.openWritableChannel( new File( migrationDir, file.getName() ) ) )
            {
                writer.writeLogHeader( channel, header );
                writer.writeAllLogEntries( channel, cursor );
            }
        }
    }

    public void moveLogs( File migrationDir, File storeDir ) throws IOException
    {
        File[] logFiles = fs.listFiles( migrationDir, versionedLegacyLogFilesFilter );
        for ( File file : logFiles )
        {
            final File originalFile = new File( storeDir, file.getName() );
            if ( originalFile.exists() )
            {
                fs.deleteFile( originalFile );
            }
            fs.moveToDirectory( file, storeDir );
        }
    }

    public void renameLogFiles( File storeDir ) throws IOException
    {
        // rename files
        for ( File file : fs.listFiles( storeDir, versionedLegacyLogFilesFilter ) )
        {
            final String oldName = file.getName();
            final long version = getLegacyLogVersion( oldName );
            final String newName = DEFAULT_NAME + DEFAULT_VERSION_SUFFIX + version;
            fs.renameFile( file, new File( file.getParent(), newName ) );
        }

        // delete old an unused log files
        for ( File file : fs.listFiles( storeDir, allLegacyLogFilesFilter ) )
        {
            fs.deleteFile( file );
        }
    }
}
