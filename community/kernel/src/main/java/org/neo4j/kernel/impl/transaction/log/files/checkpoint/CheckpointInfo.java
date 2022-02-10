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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryDetachedCheckpoint;
import org.neo4j.storageengine.api.StoreId;

public class CheckpointInfo
{
    private final LogPosition transactionLogPosition;
    private final LogPosition checkpointEntryPosition;
    private final LogPosition channelPositionAfterCheckpoint;
    private final LogPosition checkpointFilePostReadPosition;
    private final KernelVersion version;
    private final StoreId storeId;

    public CheckpointInfo( LogEntryDetachedCheckpoint checkpoint, LogPosition checkpointEntryPosition, LogPosition channelPositionAfterCheckpoint,
            LogPosition checkpointFilePostReadPosition )
    {
        this( checkpoint.getLogPosition(), checkpoint.getStoreId(), checkpointEntryPosition, channelPositionAfterCheckpoint, checkpointFilePostReadPosition,
                checkpoint.getVersion() );
    }

    public CheckpointInfo( LogPosition transactionLogPosition, StoreId storeId, LogPosition checkpointEntryPosition,
            LogPosition channelPositionAfterCheckpoint, LogPosition checkpointFilePostReadPosition, KernelVersion version )
    {
        this.transactionLogPosition = transactionLogPosition;
        this.storeId = storeId;
        this.checkpointEntryPosition = checkpointEntryPosition;
        this.channelPositionAfterCheckpoint = channelPositionAfterCheckpoint;
        this.checkpointFilePostReadPosition = checkpointFilePostReadPosition;
        this.version = version;
    }

    public LogPosition getTransactionLogPosition()
    {
        return transactionLogPosition;
    }

    public LogPosition getCheckpointEntryPosition()
    {
        return checkpointEntryPosition;
    }

    public LogPosition getChannelPositionAfterCheckpoint()
    {
        return channelPositionAfterCheckpoint;
    }

    public StoreId storeId()
    {
        return storeId;
    }

    public LogPosition getCheckpointFilePostReadPosition()
    {
        return checkpointFilePostReadPosition;
    }

    public KernelVersion getVersion()
    {
        return version;
    }

    @Override
    public String toString()
    {
        return "CheckpointInfo{" + "transactionLogPosition=" + transactionLogPosition + ", checkpointEntryPosition=" + checkpointEntryPosition +
                ", channelPositionAfterCheckpoint=" + channelPositionAfterCheckpoint + ", checkpointFilePostReadPosition=" + checkpointFilePostReadPosition +
                ", storeId=" + storeId + '}';
    }
}
