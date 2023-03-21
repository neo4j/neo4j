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
package org.neo4j.kernel.impl.transaction.log.entry.v57;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_7;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

public class LogEntryDetachedCheckpointV5_7 extends LogEntryDetachedCheckpointV5_0 {

    public LogEntryDetachedCheckpointV5_7(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            LogPosition logPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        super(DETACHED_CHECK_POINT_V5_7, kernelVersion, transactionId, logPosition, checkpointMillis, storeId, reason);
    }

    @Override
    public String toString() {
        return "LogEntryDetachedCheckpointV5_7{" + "transactionId="
                + transactionId + ", logPosition="
                + logPosition + ", checkpointTime="
                + checkpointTime + ", storeId="
                + storeId + ", reason='"
                + reason + '\'' + ", version="
                + kernelVersion() + '}';
    }
}
