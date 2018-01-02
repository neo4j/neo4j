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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.CommandWriter;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.PhysicalWritableLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderWriter.writeLogHeader;

public class StoreMigratorCheckPointer
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystem;

    public StoreMigratorCheckPointer( File storeDir, FileSystemAbstraction fileSystem )
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
    }

    /**
     * Write a check point in the log file with the given version
     *
     * It will create the file with header containing the log version and lastCommittedTx given as arguments
     *
     * @param logVersion the log version to open
     * @param lastCommittedTx the last committed tx id
     */
    public void checkPoint( long logVersion, long lastCommittedTx) throws IOException
    {
        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fileSystem );
        File logFileForVersion = logFiles.getLogFileForVersion( logVersion );
        if ( !fileSystem.fileExists( logFileForVersion ) )
        {
            fileSystem.create( logFileForVersion );
            writeLogHeader( fileSystem, logFileForVersion, logVersion, lastCommittedTx );
        }

        try ( LogVersionedStoreChannel storeChannel =
                      PhysicalLogFile.openForVersion( logFiles, fileSystem, logVersion ) )
        {
            long offset = storeChannel.size();
            storeChannel.position( offset );
            try ( PhysicalWritableLogChannel channel = new PhysicalWritableLogChannel( storeChannel ) )
            {
                final TransactionLogWriter writer =
                        new TransactionLogWriter( new LogEntryWriter( channel, new CommandWriter( channel ) ) );
                writer.checkPoint( new LogPosition( logVersion, offset ) );
            }
        }
    }
}
