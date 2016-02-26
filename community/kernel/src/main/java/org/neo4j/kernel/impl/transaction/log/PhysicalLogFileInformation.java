/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

public class PhysicalLogFileInformation implements LogFileInformation
{
    public interface LogVersionToTimestamp
    {
        long getTimestampForVersion( long version ) throws IOException;
    }


    public interface LastEntryInLog
    {
        long getLastEntryId(  );
    }

    private final PhysicalLogFiles logFiles;
    private final LogHeaderCache logHeaderCache;
    private final LastEntryInLog lastEntryInLog;
    private final LogVersionToTimestamp logVersionToTimestamp;

    public PhysicalLogFileInformation( PhysicalLogFiles logFiles,
                                       LogHeaderCache logHeaderCache,
                                       LastEntryInLog lastEntryInLog,
                                       LogVersionToTimestamp logVersionToTimestamp )
    {
        this.logFiles = logFiles;
        this.logHeaderCache = logHeaderCache;
        this.lastEntryInLog = lastEntryInLog;
        this.logVersionToTimestamp = logVersionToTimestamp;
    }

    @Override
    public long getFirstExistingEntryId() throws IOException
    {
        long version = logFiles.getHighestLogVersion();
        long candidateFirstTx = -1;
        while ( logFiles.versionExists( version ) )
        {
            candidateFirstTx = getFirstEntryId( version );
            version--;
        }
        version++; // the loop above goes back one version too far.

        // OK, so we now have the oldest existing log version here. Open it and see if there's any transaction
        // in there. If there is then that transaction is the first one that we have.
        return logFiles.hasAnyEntries( version ) ? candidateFirstTx : -1;
    }

    @Override
    public long getFirstEntryId( long version ) throws IOException
    {
        Long logHeader = logHeaderCache.getLogHeader( version );
        if ( logHeader != null )
        {   // It existed in cache
            return logHeader + 1;
        }

        // Wasn't cached, go look for it
        if ( logFiles.versionExists( version ) )
        {
            long previousVersionLastCommittedTx = logFiles.extractHeader( version ).lastCommittedTxId;
            logHeaderCache.putHeader( version, previousVersionLastCommittedTx );
            return previousVersionLastCommittedTx + 1;
        }
        return -1;
    }

    @Override
    public long getLastEntryId()
    {
        return lastEntryInLog.getLastEntryId();
    }

    @Override
    public long getFirstStartRecordTimestamp( long version ) throws IOException
    {
        return logVersionToTimestamp.getTimestampForVersion( version );
    }
}
