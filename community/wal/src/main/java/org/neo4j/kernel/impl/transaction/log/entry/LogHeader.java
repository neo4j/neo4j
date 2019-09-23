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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.util.Objects;

import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_LOG_FORMAT_VERSION;

public class LogHeader
{
    /**
     * The size of the version section of the header
     */
    public static final int LOG_HEADER_VERSION_SIZE = Long.BYTES;

    /**
     * The total size of the current header format.
     *
     * <pre>
     *   |<-                      LOG_HEADER_SIZE                  ->|
     *   |<-LOG_HEADER_VERSION_SIZE->|                               |
     *   |-----------------------------------------------------------|
     *   |          version          | last tx | store id | reserved |
     *  </pre>
     */
    public static final int LOG_HEADER_SIZE = LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;

    public final byte logFormatVersion;
    public final long logVersion;
    public final long lastCommittedTxId;
    public final StoreId storeId;

    public LogHeader( long logVersion, long lastCommittedTxId, StoreId storeId )
    {
        this( CURRENT_LOG_FORMAT_VERSION, logVersion, lastCommittedTxId, storeId );
    }

    public LogHeader( byte logFormatVersion, long logVersion, long lastCommittedTxId )
    {
        this( logFormatVersion, logVersion, lastCommittedTxId, StoreId.UNKNOWN );
    }

    public LogHeader( byte logFormatVersion, long logVersion, long lastCommittedTxId, StoreId storeId )
    {
        this.logFormatVersion = logFormatVersion;
        this.logVersion = logVersion;
        this.lastCommittedTxId = lastCommittedTxId;
        this.storeId = storeId;
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
                Objects.equals( storeId, logHeader.storeId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logFormatVersion, logVersion, lastCommittedTxId, storeId );
    }

    @Override
    public String toString()
    {
        return "LogHeader{" +
                "logFormatVersion=" + logFormatVersion +
                ", logVersion=" + logVersion +
                ", lastCommittedTxId=" + lastCommittedTxId +
                ", storeId=" + storeId +
                '}';
    }
}
