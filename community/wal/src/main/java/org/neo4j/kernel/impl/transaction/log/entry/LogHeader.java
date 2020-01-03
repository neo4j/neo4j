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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.util.Objects;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;

public class LogHeader
{
    /**
     * The size of the version section of the header
     */
    public static final int LOG_HEADER_VERSION_SIZE = Long.BYTES;

    private final byte logFormatVersion;
    private final long logVersion;
    private final long lastCommittedTxId;
    private final StoreId storeId;
    private final LogPosition startPosition;

    public LogHeader( long logVersion, long lastCommittedTxId, StoreId storeId )
    {
        this( CURRENT_LOG_FORMAT_VERSION, logVersion, lastCommittedTxId, storeId, CURRENT_FORMAT_LOG_HEADER_SIZE );
    }

    public LogHeader( byte logFormatVersion, long logVersion, long lastCommittedTxId, long headerSize )
    {
        this( logFormatVersion, logVersion, lastCommittedTxId, StoreId.UNKNOWN, headerSize );
    }

    public LogHeader( byte logFormatVersion, long logVersion, long lastCommittedTxId, StoreId storeId, long headerSize )
    {
        this.logFormatVersion = logFormatVersion;
        this.logVersion = logVersion;
        this.lastCommittedTxId = lastCommittedTxId;
        this.storeId = storeId;
        this.startPosition = new LogPosition( logVersion, headerSize );
    }

    public LogPosition getStartPosition()
    {
        return startPosition;
    }

    public byte getLogFormatVersion()
    {
        return logFormatVersion;
    }

    public long getLogVersion()
    {
        return logVersion;
    }

    public long getLastCommittedTxId()
    {
        return lastCommittedTxId;
    }

    public StoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        LogHeader logHeader = (LogHeader) o;
        return logFormatVersion == logHeader.logFormatVersion &&
                logVersion == logHeader.logVersion &&
                lastCommittedTxId == logHeader.lastCommittedTxId &&
                Objects.equals( storeId, logHeader.storeId ) &&
                Objects.equals( startPosition, logHeader.startPosition );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logFormatVersion, logVersion, lastCommittedTxId, storeId, startPosition );
    }

    @Override
    public String toString()
    {
        return "LogHeader{" +
                "logFormatVersion=" + logFormatVersion +
                ", logVersion=" + logVersion +
                ", lastCommittedTxId=" + lastCommittedTxId +
                ", storeId=" + storeId +
                ", startPosition=" + startPosition +
                '}';
    }
}
