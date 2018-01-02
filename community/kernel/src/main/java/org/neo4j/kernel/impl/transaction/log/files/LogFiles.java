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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Main point of access to database transactional logs.
 * Provide access to low level file based operations, log file headers, {@link LogFile}
 * and {@link PhysicalLogVersionedStoreChannel}
 */
public interface LogFiles extends Lifecycle
{
    long getLogVersion( File historyLogFile );

    long getLogVersion( String historyLogFilename );

    File[] logFiles();

    boolean isLogFile( File file );

    File logFilesDirectory();

    File getLogFileForVersion( long version );

    File getHighestLogFile();

    long getHighestLogVersion();

    long getLowestLogVersion();

    LogHeader extractHeader( long version ) throws IOException;

    PhysicalLogVersionedStoreChannel openForVersion( long version ) throws IOException;

    boolean versionExists( long version );

    boolean hasAnyEntries( long version );

    void accept( LogVersionVisitor visitor );

    void accept( LogHeaderVisitor visitor ) throws IOException;

    LogFile getLogFile();

    TransactionLogFileInformation getLogFileInformation();
}
