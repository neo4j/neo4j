/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.files;

import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.storageengine.api.TransactionId;

public class LogTailInformation implements LogTailMetadata
{
    public final CheckpointInfo lastCheckPoint;
    public final long firstTxIdAfterLastCheckPoint;
    public final boolean filesNotFound;
    public final long currentLogVersion;
    public final byte latestLogEntryVersion;
    private final boolean recordAfterCheckpoint;
    private final LegacyStoreId storeId;
    private final DbmsRuntimeRepository dbmsRuntimeRepository;

    public LogTailInformation( boolean recordAfterCheckpoint, long firstTxIdAfterLastCheckPoint, boolean filesNotFound, long currentLogVersion,
            byte latestLogEntryVersion, DbmsRuntimeRepository dbmsRuntimeRepository )
    {
        this( null, recordAfterCheckpoint, firstTxIdAfterLastCheckPoint, filesNotFound, currentLogVersion, latestLogEntryVersion, LegacyStoreId.UNKNOWN,
                dbmsRuntimeRepository );
    }

    public LogTailInformation( CheckpointInfo lastCheckPoint, boolean recordAfterCheckpoint, long firstTxIdAfterLastCheckPoint, boolean filesNotFound,
            long currentLogVersion, byte latestLogEntryVersion, LegacyStoreId storeId, DbmsRuntimeRepository dbmsRuntimeRepository )
    {
        this.lastCheckPoint = lastCheckPoint;
        this.firstTxIdAfterLastCheckPoint = firstTxIdAfterLastCheckPoint;
        this.filesNotFound = filesNotFound;
        this.currentLogVersion = currentLogVersion;
        this.latestLogEntryVersion = latestLogEntryVersion;
        this.recordAfterCheckpoint = recordAfterCheckpoint;
        this.storeId = storeId;
        this.dbmsRuntimeRepository = dbmsRuntimeRepository;
    }

    public boolean commitsAfterLastCheckpoint()
    {
        return recordAfterCheckpoint;
    }

    @Override
    public boolean logsMissing()
    {
        return lastCheckPoint == null && filesNotFound;
    }

    @Override
    public boolean hasUnreadableBytesInCheckpointLogs()
    {
        return lastCheckPoint != null && !lastCheckPoint.getChannelPositionAfterCheckpoint().equals( lastCheckPoint.getCheckpointFilePostReadPosition() );
    }

    @Override
    public boolean isRecoveryRequired()
    {
        return recordAfterCheckpoint || logsMissing() || hasUnreadableBytesInCheckpointLogs();
    }

    @Override
    public LegacyStoreId getStoreId()
    {
        return storeId;
    }

    @Override
    public CheckpointInfo getLastCheckPoint()
    {
        return lastCheckPoint;
    }

    @Override
    public String toString()
    {
        return "LogTailInformation{" + "lastCheckPoint=" + lastCheckPoint + ", firstTxIdAfterLastCheckPoint=" + firstTxIdAfterLastCheckPoint +
                ", filesNotFound=" + filesNotFound + ", currentLogVersion=" + currentLogVersion + ", latestLogEntryVersion=" +
                latestLogEntryVersion + ", recordAfterCheckpoint=" + recordAfterCheckpoint + '}';
    }

    @Override
    public long getCheckpointLogVersion()
    {
        if ( lastCheckPoint == null )
        {
            return EMPTY_LOG_TAIL.getCheckpointLogVersion();
        }
        return lastCheckPoint.getChannelPositionAfterCheckpoint().getLogVersion();
    }

    @Override
    public KernelVersion getKernelVersion()
    {
        if ( lastCheckPoint == null )
        {
            // there was no checkpoint since it's the first start, or we restart after logs removal, and we should use version that is defined in the system
            return dbmsRuntimeRepository.getVersion().kernelVersion();
        }
        return lastCheckPoint.getVersion();
    }

    @Override
    public long getLogVersion()
    {
        return filesNotFound ? EMPTY_LOG_TAIL.getLogVersion() : currentLogVersion;
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        if ( lastCheckPoint == null )
        {
            return EMPTY_LOG_TAIL.getLastCommittedTransaction();
        }
        return lastCheckPoint.getTransactionId();
    }

    @Override
    public LogPosition getLastTransactionLogPosition()
    {
        if ( lastCheckPoint == null )
        {
            return EMPTY_LOG_TAIL.getLastTransactionLogPosition();
        }
        return lastCheckPoint.getTransactionLogPosition();
    }
}
