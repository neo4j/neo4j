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
package org.neo4j.kernel.impl.transaction.log.entry.v50;

import java.util.Objects;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractLogEntry;
import org.neo4j.storageengine.api.LegacyStoreId;
import org.neo4j.storageengine.api.TransactionId;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;

public class LogEntryDetachedCheckpointV5_0 extends AbstractLogEntry
{
    private final TransactionId transactionId;
    private final LogPosition logPosition;
    private final long checkpointTime;
    private final LegacyStoreId storeId;
    private final String reason;

    public LogEntryDetachedCheckpointV5_0( KernelVersion version, TransactionId transactionId, LogPosition logPosition, long checkpointMillis,
            LegacyStoreId storeId, String reason )
    {
        super( version, DETACHED_CHECK_POINT_V5_0 );
        this.transactionId = transactionId;
        this.logPosition = logPosition;
        this.checkpointTime = checkpointMillis;
        this.storeId = storeId;
        this.reason = reason;
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
        LogEntryDetachedCheckpointV5_0 that = (LogEntryDetachedCheckpointV5_0) o;
        return checkpointTime == that.checkpointTime && Objects.equals( transactionId, that.transactionId ) &&
                Objects.equals( logPosition, that.logPosition ) && Objects.equals( storeId, that.storeId ) && Objects.equals( reason, that.reason );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( getVersion(), transactionId, logPosition, checkpointTime, storeId, reason );
    }

    public LegacyStoreId getStoreId()
    {
        return storeId;
    }

    public LogPosition getLogPosition()
    {
        return logPosition;
    }

    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    public String getReason()
    {
        return reason;
    }

    @Override
    public String toString()
    {
        return "LogEntryDetachedCheckpointV5_0{" + "transactionId=" + transactionId + ", logPosition=" + logPosition + ", checkpointTime=" + checkpointTime +
                ", storeId=" + storeId + ", reason='" + reason + '\'' + '}';
    }
}
