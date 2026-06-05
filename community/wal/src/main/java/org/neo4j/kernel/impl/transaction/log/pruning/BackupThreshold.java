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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;

import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class BackupThreshold implements LogPruneThreshold {
    private final long minSize;
    private final FileSizeThreshold sizeThreshold;
    private final InternalLog log;
    private volatile long backupAppendIndex = UNKNOWN_APPEND_INDEX;

    public BackupThreshold(InternalLogProvider logProvider, long minSize, FileSizeThreshold sizeThreshold) {
        this.log = logProvider.getLog(getClass());
        this.minSize = minSize;
        this.sizeThreshold = sizeThreshold;
    }

    public void setBackupAppendIndex(long backupAppendIndex) {
        this.backupAppendIndex = backupAppendIndex;
    }

    @Override
    public PrunePredicate forCycle(long lastEntryAppendIndex) {
        long capturedBackupIndex = backupAppendIndex;
        FileSizeThreshold.Predicate sizePredicate = sizeThreshold.forCycle(lastEntryAppendIndex);
        long minSizeSnapshot = minSize;
        return current -> {
            if (capturedBackupIndex == UNKNOWN_APPEND_INDEX) {
                return sizePredicate.isLowestVersionToKeep(current);
            }
            try {
                if (sizePredicate.isLowestVersionToKeep(current)) {
                    return true;
                }
                long previousFileLastAppendIndex = current.getPreviousAppendIndexFromHeader();
                if (previousFileLastAppendIndex == -1) {
                    log.warn(
                            "Failed to get append index of the first entry in the transaction log file. Requested version: "
                                    + current.version());
                    return false;
                }
                return sizePredicate.currentSize() > minSizeSnapshot
                        && previousFileLastAppendIndex < capturedBackupIndex;
            } catch (IOException e) {
                log.warn(
                        "Error on attempt to calculate reachability threshold from transaction log files. Checked version: "
                                + current.version(),
                        e);
                return false;
            }
        };
    }

    @Override
    public String toString() {
        return "backup" + " " + backupAppendIndex + (minSize > 0 ? " " + minSize + " size" : StringUtils.EMPTY)
                + " "
                + sizeThreshold;
    }
}
