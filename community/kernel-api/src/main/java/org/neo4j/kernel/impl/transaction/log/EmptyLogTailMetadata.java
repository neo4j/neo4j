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
package org.neo4j.kernel.impl.transaction.log;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.storageengine.api.TransactionId;

import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

public class EmptyLogTailMetadata implements LogTailMetadata
{
    static final LogPosition START_POSITION = new LogPosition( INITIAL_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET );

    @Override
    public boolean isRecoveryRequired()
    {
        return false;
    }

    @Override
    public long getCheckpointLogVersion()
    {
        return INITIAL_LOG_VERSION;
    }

    @Override
    public KernelVersion getKernelVersion()
    {
        return KernelVersion.LATEST;
    }

    @Override
    public long getLogVersion()
    {
        return INITIAL_LOG_VERSION;
    }

    @Override
    public LegacyStoreId getStoreId()
    {
        return LegacyStoreId.UNKNOWN;
    }

    @Override
    public boolean logsMissing()
    {
        return true;
    }

    @Override
    public TransactionId getLastCommittedTransaction()
    {
        return EMPTY_LAST_TRANSACTION;
    }

    @Override
    public LogPosition getLastTransactionLogPosition()
    {
        return START_POSITION;
    }

    @Override
    public boolean hasUnreadableBytesInCheckpointLogs()
    {
        return false;
    }

    @Override
    public CheckpointInfo getLastCheckPoint()
    {
        return null;
    }
}
