/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.recovery;

import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class RecoveryStartInformation {
    static final RecoveryStartInformation NO_RECOVERY_REQUIRED =
            new RecoveryStartInformation(LogPosition.UNSPECIFIED, null, -1);
    static final RecoveryStartInformation MISSING_LOGS = new RecoveryStartInformation(null, null, -1, true);

    private final long firstAppendIndexAfterLastCheckPoint;
    private final LogPosition transactionLogPosition;
    private final CheckpointInfo checkpointInfo;
    private final boolean missingLogs;

    public RecoveryStartInformation(
            LogPosition transactionLogPosition,
            CheckpointInfo checkpointInfo,
            long firstAppendIndexAfterLastCheckPoint) {
        this(transactionLogPosition, checkpointInfo, firstAppendIndexAfterLastCheckPoint, false);
    }

    private RecoveryStartInformation(
            LogPosition transactionLogPosition,
            CheckpointInfo checkpointInfo,
            long firstAppendIndexAfterLastCheckPoint,
            boolean missingLogs) {
        this.firstAppendIndexAfterLastCheckPoint = firstAppendIndexAfterLastCheckPoint;
        this.transactionLogPosition = transactionLogPosition;
        this.checkpointInfo = checkpointInfo;
        this.missingLogs = missingLogs;
    }

    public boolean isRecoveryRequired() {
        return transactionLogPosition != LogPosition.UNSPECIFIED;
    }

    long getFirstAppendIndexAfterLastCheckPoint() {
        return firstAppendIndexAfterLastCheckPoint;
    }

    LogPosition getTransactionLogPosition() {
        return transactionLogPosition;
    }

    public LogPosition getCheckpointPosition() {
        return checkpointInfo != null ? checkpointInfo.checkpointEntryPosition() : LogPosition.UNSPECIFIED;
    }

    public CheckpointInfo getCheckpointInfo() {
        return checkpointInfo;
    }

    boolean isMissingLogs() {
        return missingLogs;
    }
}
